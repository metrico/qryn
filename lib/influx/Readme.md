# Influx parser powered by TinyGo / WASM

## API

`async init()` - initialize WASM

`parse('<INFLUX LINE REQUEST>')` - parse influx request to JSON

- output: `[{timestamp: "<timestmap in ns>", measurement: "<measurement>", tags:{tag1: "val1"}, fields:{f1: "v1"}]`

NOTE: Currently supports only `ns` precision!!!

## Example

```javascript
const parser = require('./index');
(async() => {
    await parser.init();
    console.log(parser.parse(`m1,t1=v1,t2=v2 message="message with spaces 
and linebreaks" 1675254420130588000`));
})();
```

## Build

### Prerequisites
- golang 1.19
- tiny-go v0.26.0

### Build cmd

`tinygo build -o wasm.wasm -target wasm .`
