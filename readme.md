## Documentation of Solution approach To Cloud Data Engineer Challenge - Oyetola Taiwo


The architecture design for this pipeline namde _**pipeline-architecture.jpg**_ is present in the root folder of the sent zip file. It shows how the pipeline is able to process the inputs and give the desired output.

I have built this to run on Google Cloud 

> If we think of scaling and process a lot of jsonl files, we would need Google Cloud Apache Beam SDK to allow us perform batch and stream process. This allows for getting huge chunk of data processed and transformed easily


The requirement pointed out the need to automatic triggers of components. This why _Apache beam SDK_ would be great to use. I chose to use Apache beam with Google Cloud component _**Dataflow**_ which is essentially used to build 