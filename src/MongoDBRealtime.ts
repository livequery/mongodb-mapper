import { MongoClient } from 'mongodb'
import { LivequeryBaseEntity, DatabaseEvent } from '@livequery/types'
import { Observable, from, lastValueFrom, toArray } from 'rxjs'

export type ConnectionOptions = {
    url: string,
    database: string
}


export const listenMongoDBDataChange = <T extends LivequeryBaseEntity = LivequeryBaseEntity>({ url, database }: ConnectionOptions) => new Observable<DatabaseEvent<T>>(o => {
    setTimeout(async () => {

        while (true) {
            const connection = await MongoClient.connect(url);
            const db = await connection.db(database);
            const collections = await lastValueFrom(from(db.listCollections()).pipe(toArray()))
            for (const { name: collMod } of collections) {
                await db.command({ collMod, recordPreImages: true });
            }

            db
                .watch([], {
                    fullDocument: 'updateLookup',
                    fullDocumentBeforeChange: 'whenAvailable'
                })
                .on('error', console.error)
                .on('change', (change: any) => {
                    o.next({
                        table: change.ns.coll,
                        type: change.operationType,
                        new_data: change.fullDocument,
                        old_data: change.fullDocumentBeforeChange
                    })
                })

            await new Promise<void>(s => {
                connection.on('close', s)
                connection.on('connectionCheckOutFailed', s)
                connection.on('connectionClosed', s)
            })

            await connection.close()
        }

    })
})
