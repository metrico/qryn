import {DataStream} from "scramjet";

export namespace registry_types {
    interface Request {
        ctx?: {[k: string]: any},
        with?: {[k: string]: Request | UnionRequest}
        select: string[],
        from: string,
        left_join?: [{
            name: string,
            on: (string | string[])[]
        }],
        where?: (string | string[])[],
        limit?: number,
        offset?: number,
        order_by?: {
            name: string[],
            order: string
        },
        group_by?: string[],
        matrix?: boolean,
        stream?: ((DataStream) => DataStream)[],
        final: boolean,
        distinct?: boolean
    }
    interface UnionRequest {
        requests: Request[]
    }
}