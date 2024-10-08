import { MongoClient, ObjectId } from 'mongodb'
import { LivequeryBaseEntity, DatabaseEvent } from '@livequery/types'
import { Observable, Subject, bufferTime, filter, from, lastValueFrom, map, mergeAll, mergeMap, toArray } from 'rxjs'

export type ConnectionOptions = {
    url: string,
    database_selector?: (name:string) => boolean  
}

export type MongodbDocumentEvent = {
    operationType: string,
    ns: { db: string, coll: string }
    documentKey: { _id: ObjectId }
    fullDocumentBeforeChange: { _id: ObjectId }
    fullDocument: { _id: ObjectId }
}




export const listenMongoDBDataChange = <T extends LivequeryBaseEntity = LivequeryBaseEntity>({ url, database_selector = (name:string) => ! ['admin', 'config', 'local'].includes(name) }: ConnectionOptions) => new Observable<DatabaseEvent<T>>(o => {
    const collections = new Map<string, { db: string, coll: string }>()
    const $new_collection = new Subject<{ db: string, coll: string }>()

    setTimeout(async () => {
        const system_dbs = ['admin', 'config', 'local']

        while (true) {
            const connection = await MongoClient.connect(url);

            const s = $new_collection.pipe(
                bufferTime(3000),
                map(items => items.reduce((p, c) => {
                    p.set(c.db, new Set([
                        ...p.get(c.db) || [],
                        c.coll
                    ]))
                    return p
                }, new Map<string, Set<string>>())
                ),
                map(m => [...m]),
                mergeAll(),
                filter(([db]) => database_selector(db)),
                mergeMap(async ([db_name, list]) => {
                    const db = await connection.db(db_name);
                    for (const collMod of list) {
                        collections.set(`${db_name}|${collMod}`, { coll: collMod, db: db_name })
                        await db.command({ collMod, changeStreamPreAndPostImages: { enabled: true } })
                    }
                })
            ).subscribe()

            // list database
            const admin = connection.db("admin");
            const result = await admin.command({ listDatabases: 1, nameOnly: true });
            for (const { name } of result.databases) {
                if (database_selector(name)) {
                    const db = await connection.db(name);
                    const list = await lastValueFrom(from(db.listCollections()).pipe(toArray()))
                    for (const { name: collection_name } of list) {
                        $new_collection.next({ coll: collection_name, db: name })
                    }
                }
            }



            connection
                .watch([], {
                    fullDocument: 'updateLookup',
                    fullDocumentBeforeChange: 'whenAvailable'
                })
                .on('error', console.error)
                .on('change', (change: MongodbDocumentEvent) => {
                    change.operationType == 'insert' && !collections.has(`${change.ns.db}|${change.ns.coll}`) && $new_collection.next(change.ns)
                    const types = {
                        insert: 'added',
                        update: 'modified',
                        delete: 'removed'
                    }

                    if (types[change.operationType]) {
                        const id = change.documentKey?._id?.toString()
                        if (id) {
                            return o.next({
                                table: change.ns.coll,
                                type: types[change.operationType],
                                new_data: { ...change.fullDocument, _id: undefined, id } as any,
                                old_data: { ...change.fullDocumentBeforeChange, _id: undefined, id } as any
                            })
                        }
                    }



                })

            await new Promise<void>(s => {
                connection.on('close', s)
                connection.on('connectionCheckOutFailed', s)
                connection.on('connectionClosed', s)
            })

            await connection.close()
            s.unsubscribe()
        }

    })
})
