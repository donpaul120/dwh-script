# dwh-correction-script

### Installation Steps

#### Prerequisite
Ensure you have nodejs installed on your system

See https://nodejs.org/en/download/ on how to install nodejs


##### Step 1
clone the repository by running this command

- git clone https://github.com/donpaul120/dwh-script.git

##### Step 2
Install Dependencies \
Navigate into the cloned repository and run the command below
- npm install

##### Step 3
Configure your Data Source via Environmental Variables file
- Create a .env file within the cloned repository
- configure your data-source in the .env file

#### Configuration Example
```dotenv
DB_HOST=localhost
DB_PORT=3063
DB_USER=DatabaseUsername
DB_PASS=DataBasePassword
DB_NAME=DMR
DB_CLIENT=mssql
```

##### Step 4
Run Script
- npm start




#### Note

The script only understands the below columns from the data-source
```array
"DMR_METER_NO"
"DMR_DATE"
"DMR_READING"
"DMR_SOURCE"
"DMR2_LAR"
"DMR2_CONS"
"DMR_DATE_CREATED"
```
