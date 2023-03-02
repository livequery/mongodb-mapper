
import { LivequeryWebsocketSync } from '@livequery/nestjs'
import { getEntityName, getMetadatas } from '@livequery/typeorm'
import { MongoClient } from 'mongodb'

export const MongodbRealtimeMapperProvider = (options) => {
    return {
        provide: Symbol.for('MongodbRealtimeProviderWithMultipleConnections'),
        inject: [LivequeryWebsocketSync],
        useFactory: async (ws: LivequeryWebsocketSync) => {
            for (const { url, database, name = 'default' } of options) {
                if (!url) {
                    throw { code: 'MISSING_MONGODB_CONNECTION_URL' };
                }
                const collections_schema_refs = getMetadatas()
                    .map(option => ({
                        ...option,
                        connection: option.connection || 'default',
                        collection_name: getEntityName(option.entity)
                    }))
                    .filter(x => x.connection == name)
                    .filter(o => o.realtime)
                    .reduce((p, c) => {
                        const found = p.get(c.collection_name);
                        if (found && found.entity != c.entity) {
                            console.error(`Duplicate collection name on different entity`);
                            throw new Error();
                        }
                        p.set(c.collection_name, {
                            entity: c.entity,
                            schema_refs: new Set([...(found?.schema_refs || []), ...c.refs])
                        });
                        return p;
                    }, new Map());
                if (collections_schema_refs.size == 0) continue

                const connection = await MongoClient.connect(url);
                const db = await connection.db(database);
                for (const [collection_name] of collections_schema_refs) {
                    await db.command({ collMod: collection_name, recordPreImages: true });
                }
                db
                    .watch([], {
                        fullDocument: 'updateLookup',
                        fullDocumentBeforeChange: 'whenAvailable'
                    })
                    .on('error', console.error)
                    .on('change', (change: any) => {
                        const schema_refs = collections_schema_refs.get(change.ns.coll)?.schema_refs || [];
                        const fullDocument = (change.fullDocument || change.fullDocumentBeforeChange);
                        if (!fullDocument.id)
                            return;
                        for (const schema_ref of schema_refs) {
                            const ref = schema_ref.split('/').map((el, i) => i % 2 == 0 ? el : fullDocument?.[el]).join('/');
                            if (ref.includes('//')) continue
                            change.operationType == 'insert' && ws.changes.next({ data: fullDocument, ref, type: 'added' });
                            change.operationType == 'update' && ws.changes.next({
                                data: {
                                    ...change.updateDescription.updatedFields,
                                    id: fullDocument.id
                                },
                                ref,
                                type: 'modified'
                            });
                            change.operationType == 'delete' && ws.changes.next({ data: { id: fullDocument.id }, ref, type: 'removed' });
                        }
                    })
            }
        }
    };
} 