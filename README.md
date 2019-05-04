# BigDataGraphClustering
Label Propagation Algorithm Implementation and analysis of the results
## Students Participating 

* Imad Eddine Ibrahim Bekkouch 
* Daria Zapekina 
* Gcinizwe Dlamini

Big Data Course

Data Science Masters

Innopolis University

Innopolis

## Getting Started
Please follow the instructions to get an up and running version of our code running on your local machine.
### Prerequisites
Please make sure you have the following installed.

1. Python 3.6
2. Scala 2.12.7
3. BeautifulSoup 4.6.3
4. Numpy 1.15+
5. Pandas 0.24.1

### Running the code
First step is to download data and put it in folder 

Run the main file with using any of the following arguments:
```
usage: spark-submit LabelPropagation.jar Main <data>
<data> : string representing the location of the csv file containg the data

```

## Code structure

Quick explanation of the code structure:

    .
    ├── out                         # Output folder for the artifacts
    │   └── artifcats               # artifacts
    │       └──LabelPropagation_jar # jar file
    ├── src                         # Code
    │   ├── main                    # Source Code
    │   │   ├──data                 # Data folder
    │   │   ├──python               # python scripts
    │   │   └──scala                # Scala code
    │   └── test                    # Testing
    ├── LICENSE                     # MIT LICENCE
    ├── max.tx                      # Nodes belonging to the biggest cluster
    ├── min.txt                     # Nodes belonging to the smallest cluster
    ├── clustering.txt              # cluserting output
    ├── build.sbt                   # build configuration for project
    └── README.md                   # README
    
    
## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details