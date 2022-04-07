export namespace alerting {
    interface group {
        name: string,
        interval: string,
        rules?: rule[]
    }
    interface objGroup {
        name: string,
        interval: string,
        rules: {[key: string]: rule}
    }
    interface groupName {
        type?: string
        ns: string,
        group: string
    }
    interface ruleName {
        type?: string,
        ns: string,
        group: string,
        rule: string
    }
    interface rule {
        alert: string,
        expr: string,
        for: string,
        ver: string,
        annotations: {
            [key: string]: string
        }
        labels: {
            [key: string]: string
        }
    }
}