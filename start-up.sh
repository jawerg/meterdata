brew tap clickhouse/clickhouse
brew install clickhouse
brew services start clickhouse

brew install postgresql@14
brew services start postgresql@14

brew install awscli
aws s3 cp s3://jan-public-bucket/meterdata.zip data/raw/
unzip data/raw/meterdata.zip
