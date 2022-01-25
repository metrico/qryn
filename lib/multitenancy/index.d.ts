export interface Tenant {
    db: string
    samplesDays: number
    timeSeriesDays: number
    storagePolicy?: string
}

export function getClient(org?: string): any
export function init()
export function registerFastify(fastify: any): any
export function stop()
