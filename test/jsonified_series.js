const {createPoints, sendPoints} = require("./common");

async function main () {
    const points = createPoints("JJJ_json", 1, Date.now() - 10 * 60 * 1000, Date.now(),
        {fmt: "json_seried", lbl_repl: "val_repl", int_lbl: "1"}, {},
        (i) => JSON.stringify({fff: {i1: 1*(i%2), i2:2*(i%3), i3: 3*(i%4)}})
    );

    await sendPoints('http://localhost:3100', points);

}
main().then(() => console.log("OK"));