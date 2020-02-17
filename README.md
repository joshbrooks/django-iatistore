# django-iatistore

IATI data comes in XML. Great! We can use a schema to nicely validate it, anyone can publish it.

But getting data OUT of XML is hard and getting data from a million XML files is not fun.

Postgres to the rescue.

# Building Proto

## Setup

nvm use stable
npm i protoc protoc-gen-ts

## Compile

```
cd iatistore && \
protoc --python_out=. transaction.proto
```

For JS / TS generation:

```
nvm use stable && \
cd iatistore && \
protoc --plugin=protoc-gen-ts=/home/josh/node_modules/.bin/protoc-gen-ts --ts_out=. --js_out=import_style=commonjs,binary:. transaction.proto
```
