const express = require('express');
const app = express()
const childProcess = require("child_process");

let chokidar;
async function init() {
    chokidar = await import('chokidar');
    chokidar.watch(__dirname + "/../*", {ignored: /^\./, persistent: true})
        .on('change', rebuild)
        .on('unlink', rebuild)
        .on('error', rebuild);

    const port = process.env.PORT || 3001
    app.listen(port, () => {
        console.log(`Started serving files on port ${port}!`)
    })
    rebuild("initial build")
}

let building = false
let triggeredDuringBuild = false
const rebuild = (path) => {
    console.log(`File ${path} has been changed, rebuilding site...`)
    if (building) {
        console.log("Triggered during build, waiting for build to finish...")
        triggeredDuringBuild = true
        return
    }
    triggeredDuringBuild = false
    building = true;
    const process = childProcess.spawn("npx", ["antora", "playbook.yaml"], {stdio: 'inherit'})
    process.on("exit", (code) => {
        if (code === 0) {
            console.log("Site rebuilt successfully!")
        } else {
            console.error("Failed to rebuild site!")
        }
        building = false
        if (triggeredDuringBuild) {
            rebuild()
        }
    })
}

app.use(express.static('build/site'))

init()
