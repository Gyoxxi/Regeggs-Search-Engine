<p align="center">
<img src="src/resources/webapp/icons/title.png" alt="Regeggs" style="width: 500px">
</p>

# Introduction of Regeggs Search Engine
Welcome to the Regeggs Search Engine repository! This project harnesses the power of a Spark-like Java web server and a tailored version of Apache Spark, which we've named Flame. Our search engine is built on a robust architecture that utilizes Flame for crawling and data processing, paired with a KVS Coordinator/Workers setup for orchestration. This combination allows us to effectively manage the crawling, indexing, and analysis of over 500,000 web pages.

- Distributed Flame Engine: Modified Apache Spark that supports RDD and PairRDD, optimizing our data handling and computational efficiency.
- Scalable Crawling: Utilizes a powerful yet polite crawling mechanism with a comprehensive list of seed URLs, ensuring a broad range of relevant search results.
- Advanced Analytics: Implements algorithms for PageRank and TFIDF calculations, providing a strong foundation for relevant and accurate search results.
- User-Friendly Interface: Hosts a search server with a beautifully designed user interface that makes navigation and usage straightforward for end-users.

# Third-party components used:
- [Apache OpenNLP](https://opennlp.apache.org/) (SentenceDetector) for analyzing page content and extracting meaningful information as snippets in search results
- [466k words.txt](https://github.com/dwyl/english-words/tree/master) for autocompleting English search terms while user typing in search bar

# How to run the code locally:
Clone the main repository

For testing purpose: we recommand keeping the amount of pages crawled small (<100k pages), please run Crawler, Indexer and PageRank classes with desired kvs and flame workers for this task. 

After completing the jobs, place the three tables under worker1/ inside the project directory, and make sure that the three tables' directories are named as: **"pt-crawl"**, **"pt-pageranks"** and **"pt-TFIDF"**, this is to ensure consistency with the default file path we used for the search server. 

In the root directory of this project, run script.sh, this will compile and run required classes together (you may specify any desired number of flame/kvs workers in script.sh):
```
bash script.sh
```

Then, run src/cis5550/search/SearchServer.java in IDE or use the command below, please ensure that port 80 is available:
```
java -cp "classes:lib/*" cis5550.search.SearchServer
```

Now open the following URL in your browser, to try out Regeggs Search Engine:
```
localhost:80
```

# How Regeggs looks like in browser
- Regeggs homepage:

  
  <img src="media/01.png" alt="Homepage" style="width: 500px">


- Search with autocomplete:

  <img src="media/02.png" alt="Typing" style="width: 500px">

- Loading component while fetching results:

  <img src="media/03.png" alt="Loading" style="width: 500px">


- Display of search results:
  
  <img src="media/04.png" alt="Results" style="width: 500px">


- Hover-over page preview:
  
  <img src="media/05.png" alt="Preview" style="width: 500px">


