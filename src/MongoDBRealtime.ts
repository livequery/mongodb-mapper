import { MongoClient, ObjectId } from 'mongodb'
import { LivequeryBaseEntity, DatabaseEvent, WebsocketSyncPayload, UpdatedData, UpdatedDataType } from '@livequery/types'
import { EMPTY, Observable, from, map, mergeAll, mergeMap, retry, tap } from 'rxjs'
import { RouteOptions, MongooseDatasourceConfig, MongooseDatasource } from '@livequery/mongoose'

export type ConnectionOptions = {
    url: string,
    sources: {
        [db: string]: Set<string>
    }
}
export type MongodbDocumentEvent = {
    operationType: string,
    ns: { db: string, coll: string }
    documentKey: { _id: ObjectId }
    fullDocumentBeforeChange: { _id: ObjectId }
    fullDocument: { _id: ObjectId }
    updateDescription: {
        updatedFields: { [key: string]: any },
        removedFields: string[]
        truncatedArrays: []
    }
}

const types = {
    insert: 'added',
    update: 'modified',
    delete: 'removed'
}

type RefMetadata = {
    collection: string
    field?: string,
    array?: boolean
}

export type LivequeryDatasourceWatcher = {
    watch(
        config: MongooseDatasourceConfig,
        routes: Array<{ path: string, method: number, options: RouteOptions }>,
        ds: MongooseDatasource
    ): Observable<UpdatedData<any>>
}
export class MongodbRealtime implements LivequeryDatasourceWatcher {

    #reformatId(obj: any) {
        if (!obj) return obj
        const { _id, __v, id, ...o } = obj
        return {
            id: _id ? _id.toString() : id || '#',
            ...Object.entries(o).reduce((p, [k, v]) => {
                if (k.startsWith('_')) return p
                return {
                    ...p,
                    [k]: v
                }
            }, {})
        }
    }
 

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
                        return new Observable<DatabaseEvent<T> & { fields: Set<string> }>(o => {
                            const s = collection.watch([], {
                                fullDocument: 'updateLookup',
                                fullDocumentBeforeChange: 'whenAvailable'
                            })
                                .on('error', console.error)
                                .on('change', (change: MongodbDocumentEvent) => {
                                    if (types[change.operationType]) {
                                        const fields = new Set(change.operationType == 'update' ? [
                                            ...Object.keys(change.updateDescription.updatedFields || {}).map(a => a.split('.')[0]),
                                            ...change.updateDescription.removedFields.map(a => a.split('.')[0])
                                        ].filter(f => !f.startsWith('_')) : [])
                                        o.next({
                                            table: change.ns.coll,
                                            type: types[change.operationType],
                                            new_data: this.#reformatId(change.fullDocument),
                                            old_data: this.#reformatId(change.fullDocumentBeforeChange),
                                            fields
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


    #format(paths: Map<string, Map<string, RefMetadata[]>>, e: DatabaseEvent<any> & { fields: Set<string> }): UpdatedData<any>[] {
        const table = paths.get(e.table)
        if (!table) return [];

        const merged = {
            ...e.old_data || {},
            ...e.new_data || {}
        } as Record<string, any> & { id: string }


        const changes = Object.keys(merged).filter(k => e.fields.has(k)).reduce((p, c) => {
            return {
                ...p,
                [c]: e.new_data?.[c]
            }
        }, { id: merged.id } as Record<string, any> & { id: string })


        const check = (field: string, path_value: string): UpdatedDataType => {
            const o = e.old_data?.[field]
            const n = e.new_data?.[field]
            const string_value = `${path_value}`
            if (Array.isArray(o) || Array.isArray(n)) {
                const oarray = (Array.isArray(o) ? o : []).map(a => `${a}`)
                const narray = (Array.isArray(n) ? n : []).map(a => `${a}`)
                if (oarray.includes(string_value) && !narray.includes(string_value)) return 'removed'
                if (!oarray.includes(string_value) && narray.includes(string_value)) return 'added'
                return 'modified'
            }
            if (`${n}` == string_value && `${o}` != string_value) return 'added'
            if (`${o}` == string_value && `${n}` != string_value) return 'removed'
            return 'modified'
        }


        const buildRef = ([{ array, collection, field }, ...fields]: RefMetadata[]): Array<{ refs: string[], type: UpdatedDataType }> => {
            if (fields.length == 0 || !field) return [{ refs: [collection], type: e.type }]
            const currents: string[] = array ? (Array.isArray(merged[field]) ? [...new Set([
                ...e.old_data?.[field]?.map((a: any) => `${a}`) || [],
                ...e.new_data?.[field]?.map((a: any) => `${a}`) || []
            ])] : ['-']) : [merged[field] ?? '-']


            return currents.map(value => {
                const nexts = fields.length == 0 ? [] : buildRef(fields)
                return nexts.map(next => {
                    const type = e.type == 'added' || e.type == 'removed' ? e.type : (
                        next.type == 'added' || next.type == 'removed' ? next.type : check(field, value)
                    )
                    return {
                        type,
                        refs: [
                            collection,
                            `${value}`,
                            ...next.refs
                        ]
                    }
                })
            }).flat(1)
        }


        return [...table.values()].map(metadatas => {
            const list = buildRef(metadatas)
            return list.map(({ refs, type }) => {
                const ref = refs.join('/')
                const data = type == 'added' ? merged : {
                    ...type == 'modified' ? changes : {},
                    id: merged.id
                }
                return { ref, type, data, }
            })
        }).flat(1)
    }

    watch(
        config: MongooseDatasourceConfig,
        routes: Array<{ path: string, method: number, options: RouteOptions }>
    ) {

        const urls = Object.values(config.connections).map(a => (a as any as { _connectionString: string })._connectionString)
        const realtime_routes = routes.filter(r => r.method == 0 && !!r.options.realtime)
        if (realtime_routes.length == 0) return EMPTY
        const collections = realtime_routes.map(r => r.options.schema.options.collection as string)
        const sources = config.databases.reduce((p, c) => {
            return {
                ...p,
                [c]: new Set(collections)
            }
        }, {} as { [db: string]: Set<string> })
        const paths = realtime_routes.reduce((p, c) => {
            const map = p.get(c.path) || new Map<string, RefMetadata[]>()
            const refs = c.path.split('/')
            const ref = refs.join('/')
            const collection_name = c.options.schema.options.collection as string
            map.set(ref, refs.map((collection, index) => {
                if (index % 2 == 1) return []
                const ref = refs[index + 1]
                const field = ref ? c.options.schema.paths[ref == 'id' ? '_id' : ref] : null
                if (field === undefined) throw new Error(`Field ${ref} not found in schema for collection ${collection_name} in path ${c.path}`)
                const array = field ? field.instance == 'Array' : undefined
                return [{ array, collection, field: ref } as RefMetadata]
            }).flat(2))
            p.set(collection_name, map)
            return p
        }, new Map<string, Map<string, RefMetadata[]>>())

        return from(urls).pipe(
            mergeMap(url => this.#listenRawChanges({ url, sources })),
            map(e => this.#format(paths, e)),
            mergeAll()
        )
    }
}
