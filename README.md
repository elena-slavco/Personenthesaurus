# Scripts for Personenthesaurus

In order to be able to publish linked data to an online data catalog, the following steps need to be followed: 

## 1. Install dependencies
Navigate into the directory containing your code and run `npm install`. This will download and install all dependencies for you to your local computer.

## 1. Create a TriplyDB API Token

**NOTE** *This step can be omitted if you already created or provided your token during setup of your project*
​
Your TriplyDB API Token is your access key to TriplyDB. You can create one in TriplyDB using [this instructions](https://docs.triply.cc/generics/api-token/) or you can type (and follow the onscreen instructions):

```sh
npx tools create-token
```

Once you have your token, open the file `.env` and write the following line:
`TRIPLYDB_TOKEN=<your-token-here>`

### 2.1 Transpile

Your Script is written in TypeScript, and will be executed in JavaScript.  The following command transpiles your TypeScript code into the corresponding JavaScript code:

```sh
npm run build
```

### 2.1.1 Continuous transpilation

Some developers do not want to repeatedly write the `npm run build` command.  By running the following command, transpilation is performed automatically whenever one or more TypeScript files are changed:

```sh
npm run dev
```

### 2.2 Run

The following command runs your scripts:

```sh
node lib/getMuziekschattenData.js && 
node lib/getMuziekwebData.js && 
node lib/getGTAAData.js && 
node lib/getKBData && 
node lib/runPipelines.js 
```
