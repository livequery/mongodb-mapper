import { LivequeryWebsocketSync } from '@livequery/nestjs';
export declare const MongodbRealtimeMapperProvider: (options: any) => {
    provide: symbol;
    inject: (typeof LivequeryWebsocketSync)[];
    useFactory: (ws: any) => Promise<void>;
};
