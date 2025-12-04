# 332project
The goal of this project is to implement a **Distributed Sorting** system in Scala, designed to sort large-scale key/value records stored across multiple machines. The system is built to handle datasets that are too large to fit in the main memory or even on a single machine's disk.
## Documents  
### Progress
Weekly Progress: 
[weekly progress](https://morning-drawbridge-ada.notion.site/2025-CSED332-Team-Gray-Weekly-Progress-2a8be201a70880eeb908fbfeaf81ee4b?source=copy_link)  
Milestones: [milestones](https://morning-drawbridge-ada.notion.site/Milestone-2a8be201a708800c8f46e07d7411c685?source=copy_link)  
### Design & Structure
Project Design: [design](https://morning-drawbridge-ada.notion.site/Design-2a8be201a708803a9b6ec997b20b5191?source=copy_link)  
Project Directory: [directory structure](https://morning-drawbridge-ada.notion.site/Project-Directory-2a8be201a70880e6950cc0d8e20270eb?source=copy_link)  
### Presentation
Progress Presentation: [progress presentation](./(SD%20gray)%20Progress%20presentation.pptx)
### How to run
Instructions for **building the project** and **running the master/worker** are described in both the documentation and this MD file ([How To Run](#how-to-run-1)).  

Connect the cluster: [connect the cluster](https://morning-drawbridge-ada.notion.site/Connect-the-cluster-2b7be201a70880d9a762fd243c23ce42?source=copy_link)  
Build project: [build project](https://morning-drawbridge-ada.notion.site/Build-project-2b6be201a7088015b77ce8048a79f03d?source=copy_link)  
Generate data: [generate data](https://morning-drawbridge-ada.notion.site/generate-data-2b1be201a708805b9fc0d2c93df80b69?source=copy_link)  
Run master, worker: [run master, worker](https://morning-drawbridge-ada.notion.site/Run-master-worker-2b6be201a708805ab648d90bd91598d6?source=copy_link)  
Validate solution: [validate solution](https://morning-drawbridge-ada.notion.site/validate-solution-2b6be201a70880578f86d937fd9fa4f7?source=copy_link)  

## How To Run
Instructions for **connecting the cluster**, **generating data**, and **validating solutions** are detailed in the [Documentation](#how-to-run).

### 1. Build Project
First, build the project on the machine where you want to run the application.

1. Clone the repository to your desired directory.
   ```bash
   git clone https://github.com/cmhong0228/332project.git
   ```
2. Navigate to the project directory.
    ```bash
    cd 332project
    ```
3. Build the project using sbt assembly.  
    ```bash
    sbt assembly
    ```
### 2. Run Master
>*Note: This step must be executed on the Master Node.*

Choose one of the following options to run the master.

#### Option 1. Using shell script
1. Nevigate to the 332project directory.  
2. Grant execution permission to the script (skip if already done).  
    ```bash
    chmod +x master
    ```
3. Run the master script.  
    Usage: `./master <# of workers>`  
    Example  
    ```bash
    ./master 20
    ```
#### Option 2. Running the JAR file directly
1. Nevigate to the 332project directory.  
2. Execute the JAR file.  
    Usage: `java -cp <jar path> distributedsorting.master.Master <# of workers>`  
    Example  
    ```bash
    java -cp ./target/scala-2.13/distributedsorting.jar distributedsorting.master.Master 20
    ```
### 3. Run Worker
>*Note: This step must be executed on each Worker Node.*

Choose one of the following options to run the worker.

#### Option 1. Using shell script
1. Nevigate to the 332project directory.  
2. Grant execution permission to the script (skip if already done).  
    ```bash
    chmod +x worker
    ```
3. Run the worker script.  
    Usage: `./worker <master ip:port> -I [input directories] -O <output directory>`  
    Example  
    ```bash
    ./worker 10.1.25.21:37094 -I /dataset/small -O ~/332project/output
    ```
#### Option 2. Running the JAR file directly
1. Nevigate to the 332project directory.
2. Execute the JAR file.  
    Usage: `java -cp <jar path> distributedsorting.worker.Worker <master ip:port> -I [input directories] -O <output directory>`  
    Example  
    ```bash
    java -cp ./target/scala-2.13/distributedsorting.jar distributedsorting.worker.Worker 10.1.25.21:37094 -I /dataset/small -O ~/332project/output
    ```