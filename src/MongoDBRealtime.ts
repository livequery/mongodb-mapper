import { MongoClient } from 'mongodb'
import { LivequeryBaseEntity } from '@livequery/types'
import { Observable, from, lastValueFrom, toArray } from 'rxjs'

export type ConnectionOptions = {
    url: string,
    database: string
}

export type LogData<T = {}> = {
    tag: 'delete' | 'update' | 'insert' | 'begin' | 'relation' | 'commit'
    relation?: {
        tag: 'relation',
        relationOid: 16395,
        schema: 'public',
        name: string,
        replicaIdentity: 'full',
        keyColumns: string[]
    },
    old: T,
    new: T,

}

export type DatabaseEvent<T> = {
    table: string
    type: 'added' | 'modified' | 'removed',
    new_data?: Partial<T>,
    old_data?: T
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
