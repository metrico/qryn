## qryn-plugin

### mongo

This plugin uses the [Mongo Query Compiler](https://github.com/aptivator/mongo-query-compiler-docs) library to transpile mongodb-like 
query objects into a JavaScript filtering functions to be used with any array's .filter() method to isolate the needed data subset.

### Example

Original LogQL Selector:
```
{type="people"} |="alive" | json
```

Mongo Filter:
```
mongo({age: {$exists: true}}, {type="people"} |="alive" | json)
```
