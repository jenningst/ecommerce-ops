# E-commerce MLOps
A repository for demonstrating MLOps architecture for Amazon e-commerce reviews.

# Citations
[1] "_Justifying recommendations using distantly-labeled reviews and fined-grained aspects_", Jianmo Ni, Jiacheng Li, Julian McAuley, Empirical Methods in Natural Language Processing (EMNLP), 2019

https://nijianmo.github.io/amazon/index.html

# To Dos
- ~~Obtain Amazon review data~~
- ~~Add Feast to dependencies~~
- Build a spaCy pipeline to generate lemmatized tokens from the review text
- Build a spaCy pipeline to create embeddings from the review text


- Store data on Azure blob storage ("raw" storage)
- Outline ingestion pipeline
    - When/how to ingest incrementally? CosmosDB for tracking dates ingested?
- Setup Feast to ingest data into feature store

# Dependencies
- Terraform for all Azure resource creation
- Feast for feature store
