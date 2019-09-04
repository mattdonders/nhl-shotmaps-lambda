# Shotmap Generator on AWS Lambda

This is a proof of concept to show that we can generate NHL shotmaps from any given NHL Game ID and send it out via a tweeet all within an AWS Serverless Lambda function.

Because of the kernel distribution calculation this function may require a bit more memory than something basic like text parsing and the Memory property is directly linked to the amount of CPU given to the function at runtime. A good balance for this specific code is setting the Memory to 1,024MB which leaves us with the following run times / memory usages (even on a cold start).

- Duration: 6220.28 ms
- Billed duration: 6300 ms
- Resources configured: 1024 MB
- Max memory used: 323 MB

## Packging for Deployment
Because certain packages (namely numpy, scipy, etc) require separate AWS `manylinux wheel` formats, we use zappa to package the bundle up for deployment into our existing Lambda function.

The other thing to note is that because of these dependencies, we exceed the size of a package that can be copied directly from the filesystem. In order to get around this restriction, we upload our code into an S3 bucket and then load the Lambda function from that uploaded zip file.

```
# Create a virtualenv to silo this project
$ mkvirtualenv lambda-shotmap
$ pip install -r requirements.txt

# Auto-Package the code (including the manylinux wheels) via Zappa
$ zappa package dev
Calling package for stage dev..
Downloading and installing dependencies..
 - scipy==1.3.0: Using locally cached manylinux wheel
 - pillow==6.1.0: Using locally cached manylinux wheel
 - pandas==0.25.0: Using locally cached manylinux wheel
 - numpy==1.17.0: Using locally cached manylinux wheel
 - matplotlib==3.1.1: Using locally cached manylinux wheel
 - kiwisolver==1.1.0: Using locally cached manylinux wheel
 - sqlite==python3: Using precompiled lambda package
Packaging project as zip.

# Copy the ZIP file int our S3 Bucket
$ aws s3 cp <zipfile> s3://<s3-bucket>/code/scraper-v1-dev.zip

# Finally load the ZIP file frmo the S3 bucket into the update-function-code command
$ aws lambda update-function-code --function-name <lambda-function-name> --s3-bucket <s3-bucket> --s3-key code/scraper-v1-dev.zip
```

## Testing / Running the Function
In order to make this is as *dynamic* as possible, most of the values that can be changed are stored in Environment Variables within the function itself. The following need to be set for this function to properly run.

**Required**
- debug_twtr_access_secret
- debug_twtr_access_token
- debug_twtr_consumer_key
- debug_twtr_consumer_secret
- S3_BUCKET (the S3 bucket that holds necessary objects)
- BLANK_SHOTMAP (the key of the blank shotmap image)

**Optional**
- GAMEID (Valid NHL Game ID, ex: 2018020020)

The NHL Game ID should be passed in via the event parameter into the handler in a dictionary with key `game_id` as per the below sample.
```python
> print(event)
{'game_id': '2018020020'}
```