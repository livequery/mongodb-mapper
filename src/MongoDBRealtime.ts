import { MongoClient, ObjectId } from 'mongodb'
import { LivequeryBaseEntity, DatabaseEvent, WebsocketSyncPayload } from '@livequery/types'
import { Observable, from, map, mergeAll, mergeMap, retry } from 'rxjs'
import { RouteOptions, MongooseDatasourceConfig, MongooseDatasource } from '@livequery/mongoose'

export type ConnectionOptions = {
    url: string,
    sources: {
        [db: string]: string[]
    }
}

export type MongodbDocumentEvent = {
    operationType: string,
    ns: { db: string, coll: string }
    documentKey: { _id: ObjectId }
    fullDocumentBeforeChange: { _id: ObjectId }
    fullDocument: { _id: ObjectId }
}

const types = {
    insert: 'added',
    update: 'modified',
    delete: 'removed'
}


export type LivequeryDatasourceWatcher = {
    watch(
        config: MongooseDatasourceConfig,
        routes: Array<{ path: string, method: number, options: RouteOptions }>,
        ds: MongooseDatasource
    ): Observable<WebsocketSyncPayload<any>>
}
export class MongodbRealtime implements LivequeryDatasourceWatcher {

    #listenRawChanges<T extends LivequeryBaseEntity = LivequeryBaseEntity>({ url, sources = {} }: ConnectionOptions) {
        return from(Object.entries(sources)).pipe(
            mergeMap(async ([dbName, list]) => {
                const connection = await MongoClient.connect(url);
                const db = await connection.db(dbName);
                return from(list).pipe(
                    mergeMap(async (coll) => {
                        const collection = db.collection(coll);
                        await db.command({
                            collMod: coll,
                            changeStreamPreAndPostImages: { enabled: true }
                        })
                        return new Observable<DatabaseEvent<T>>(o => {
                            const s = collection.watch([], {
                                fullDocument: 'updateLookup',
                                fullDocumentBeforeChange: 'whenAvailable'
                            })
                                .on('error', console.error)
                                .on('change', (change: MongodbDocumentEvent) => {
                                    if (types[change.operationType]) {
                                        const id = change.documentKey?._id?.toString()
                                        o.next({
                                            table: change.ns.coll,
                                            type: types[change.operationType],
                                            new_data: { ...change.fullDocument, _id: undefined, id } as any,
                                            old_data: { ...change.fullDocumentBeforeChange, _id: undefined, id } as any
                                        })
                                    }
                                })
                            return () => s.close()
                        })
                    }),
                    mergeMap($ => $)
                )
            }),
            retry(),
            mergeMap($ => $)
        )
    }

    watch(
        config: MongooseDatasourceConfig,
        routes: Array<{ path: string, method: number, options: RouteOptions }>
    ) {

        const urls = Object.values(config.connections).map(a => (a as any as { _connectionString: string })._connectionString)
        const collections = (
            [...routes.values()]
                .filter(r => r.method == 0 && !!r.options.realtime)
                .map(r => r.options.schema.options.collection as string)
        )
        const sources = config.databases.reduce((p, c) => {
            return {
                ...p,
                [c]: collections
            }
        }, {} as { [db: string]: string[] }) 



        const format = (e: DatabaseEvent<any>): WebsocketSyncPayload<any>[] => {
            return []
        }

        return from(urls).pipe(
            mergeMap(url => this.#listenRawChanges({ url, sources })),
            map(format),
            mergeAll(),
        )
    }
}
