# mse-advDaBa-2022
## Approach for loading the graph
We use JsonReader to read the file. We then create an Article object from each read json object. Said article is then introduced into the graph. For the references we create the referenced article in the graph but only with its ID. It will be updated when the Article is loaded. As we don't need a list of Article in the stack it takes less memory to do it this way. We simply ignore malformed data as it didn't affect data asked for the lab.  
## Parameter values picked

## Performance test
