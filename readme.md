# ddb-clean
> simple cli tool to delete data from dynamodb tables

## Usage

This tool must be used in two steps.  
1. Scan the table to get the list of items you want to delete.
2. Delete the items with the list you got from the scan.

### Scan the table

```sh
ddb-clean scan my-table createdAt 2022-12-01 2022-12-31
``` 

This will scan the table and save the list of items to json files.

### Delete the items

```sh
ddb-clean delete my-table
```
This will loop through the json files and delete the items.
Files will be deleted once the items are deleted, so you can run this command multiple times if you want to delete items in batches or in case the process fails. 

