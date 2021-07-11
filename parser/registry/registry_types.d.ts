export namespace registry_types {
    interface Request {
        select: string[],
        from: string,
        left_join?: [{
            name: string,
            on: (string | string[])[]
        }],
        where?: (string | string[])[],
        limit?: number,
        offset?: number
    }
}