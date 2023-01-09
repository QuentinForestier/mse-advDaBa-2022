# mse-advDaBa-2022
To start the lab use the `build.sh` script and then run the `docker-compose up` command.
## Approach for loading the graph
We use JsonReader to read the file. 

We then create an Article object from each json object read. 

The items are gathered in batches of 5000 (this number is modifiable according to the environment).

These batches are then introduced into the graph using threads.

Once a batch is finished it's destroyed which allows us to load only the batches being executed in memory.

For the references, we create the referenced item in the graph but only with its ID. This means that there will be more nodes in the graph than nodes specified in the environment variables. It will be updated when the article is loaded.

We simply ignore the malformed data as it has not affected the data requested for the lab.
## Parameter values picked
BATCH_SIZE=5000

MAX_NODES=50000
## Performance test
Number of articles = 96067

Number of authors = 20045

Authored relations = 23039

Citation relations = 60567

Memory in MB = 3000

Seconds = 30.349
