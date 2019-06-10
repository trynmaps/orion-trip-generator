const AWS = require('aws-sdk');
const s3 = new AWS.S3();

//AWS.config.loadFromPath('./aws-config.json');

module.exports = {
    putFileToBucket: async function (params) {
        return await new Promise((resolve, reject) => {
            s3.putObject(params, function (err, data) {
                if (err) {
                    reject(err);
                }
                else {
                    resolve({
                        status: data,
                        data: params.Body});
                }
            });
        });
    },
    listBucket: async function (params) {
        return await new Promise((resolve, reject) => {
            let allKeys = [];
            let listAllKeys = (token) => {
                if (token) {
                    params.ContinuationToken = token;
                }

                s3.listObjectsV2(params, (err, data) => {
                    if (err) throw err;
                    allKeys = allKeys.concat(data.Contents);

                    if (data.IsTruncated)
                        listAllKeys(data.NextContinuationToken);
                    else
                        resolve(allKeys);
                });
            }

            listAllKeys();
        });
    },
    readTextS3: async function (bucket, key) {
        return await new Promise((resolve, reject) => {
            let params = { Bucket: bucket, Key: key };

            s3.getObject(params, function (err, data) {
                if (err) {
                    reject(err);
                }
                else {
                    resolve(data.Body.toString());
                }
            });
        });
    }
}
