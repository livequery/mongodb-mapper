import { MongoClient, ObjectId } from 'mongodb'
import { LivequeryBaseEntity, DatabaseEvent } from '@livequery/types'
import { Observable, from, mergeAll, mergeMap, of } from 'rxjs'

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


export const listenMongoDBDataChange = <T extends LivequeryBaseEntity = LivequeryBaseEntity>({
    url,
    sources = {}
}: ConnectionOptions) => from(Object.entries(sources)).pipe(
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
    mergeMap($ => $)
) 