# E-commerce MLOps
A repository for demonstrating DataOps and MLOps architectural concepts for a toy Amazon e-commerce problem.


# To Dos
- ~~Obtain Amazon review data~~
- ~~Add data pipeline dependencies~~
- Build a data preprocessing pipeline in Prefect
    - Ingest from URL?
    - Split dataset
    - Feature-engineering
        - Build a spaCy pipeline to generate lemmatized tokens from the review text
        - Build a spaCy pipeline to create embeddings from the review text
- Store data on Azure blob storage ("raw" storage)
- Setup Feast to ingest data into feature store (from local)


# Dependencies
- Terraform for all Azure resource creation
- Feast for feature store
- Prefect for orchestration


# Citations
[1] "_Justifying recommendations using distantly-labeled reviews and fined-grained aspects_", Jianmo Ni, Jiacheng Li, Julian McAuley, Empirical Methods in Natural Language Processing (EMNLP), 2019

https://nijianmo.github.io/amazon/index.html