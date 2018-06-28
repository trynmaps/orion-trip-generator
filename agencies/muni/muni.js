const awsHelper = require('../../awsHelper')
const utils = require('../../util/utils')
const muniConfig = require('./muniConfig');

const getLatestVehicleDataAsTrips = async () => {
  return await new Promise((resolve, reject) => {
    awsHelper.listBucket(
      { Bucket: muniConfig.vehicleBucket, Prefix: "muni-nextbus-bucket_" }).then((result) => {
        let sortFn = (a, b) => {
          let tsA = Number(a.Key.split("_")[1].split(".")[0])
          let tsB = Number(b.Key.split("_")[1].split(".")[0])

          if (tsA > tsB) {
            return -1;
          }
          else {
            return 1;
          }

          return 0;
        }

        const mostRecentFile = result.sort(sortFn)[0].Key;
        awsHelper.readTextS3(muniConfig.vehicleBucket, mostRecentFile).then((result) => {
          const vehicleData = JSON.parse(result)
          const tripStartTime = Date.now();
          console.log("Loading trips from must recent muni trip file at s3://" + 
            muniConfig.vehicleBucket + "/" + mostRecentFile);
          resolve(vehicleData.map(v => {
            return {
              "agency": "muni",
              "startTime": tripStartTime,
              "endTime": null,
              "route": v.routeId,
              "direction": v.directionId,
              "vid": v.id,
              "states": [{
                "vtime": tripStartTime,
                "lat": v.lat,
                "lon": v.lon
              }]
            }
          }));
        }).catch((err) => {
          reject(err)
        });
      });
  });
}

const initTripState = () => {
  return awsHelper.readTextS3(muniConfig.stateBucket, muniConfig.stateFile).then((data) => {
    console.log("Loaded trip state from s3:", muniConfig.stateBucket, muniConfig.stateFile)
    return data;
  }).catch((err) => {
    return getLatestVehicleDataAsTrips();
  }).then((trips) => {
    awsHelper.putFileToBucket({
      Body: new Buffer(trips, 'binary'),
      Bucket: muniConfig.stateBucket,
      Key: muniConfig.stateFile
    }).then((result) => {
      console.log("Uploaded state file to s3", result);
    }).catch((err) => {
      console.log("Error uploaded state file to s3", err);
    })
  });
}

module.exports = {
  initTripState: initTripState,
  getLatestVehicleDataAsTrips: getLatestVehicleDataAsTrips
}