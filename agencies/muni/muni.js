const awsHelper = require('../../awsHelper')
const muniConfig = require('./muniConfig');

const updateTripState = (existingTrips, newTrips) => {
  return new Promise((resolve, reject) => {
    if (!newTrips || newTrips === undefined) {
      newTrips = [];
    }

    if (!existingTrips || existingTrips === undefined || existingTrips.length === 0) {
      resolve([newTrips, [], []]);
    }

    const reducer = (acc, cur) => {
      const index = newTrips.findIndex((n) => { return n.vid === cur.vid && n.route === cur.route && n.direction === cur.direction })
      //should never be more than one match
      if (index != -1) {
        const matchingTrip = newTrips[index]
        for (let i = 0; i < matchingTrip.states.length; ++i) {
          cur.states.push({
            vtime: matchingTrip.states[i].vtime,
            lat: matchingTrip.states[i].lat,
            lon: matchingTrip.states[i].lon
          })
        }

        acc.updatedExistingTrips.push(cur)
        //remove the matching trip from the array of new trips as it is not really new
        newTrips.splice(index, 1);
      }
      else {
        cur.endTime = Date.now()
        acc.endedTrips.push(cur);
      }

      return acc;
    };

    const state = existingTrips.reduce(reducer, { updatedExistingTrips: [], endedTrips: [] })

    resolve([newTrips, state.updatedExistingTrips, state.endedTrips])
  })
}

const writeTrips = (trips) => {
  let promises = [];

  for (let i = 0; i < trips.length; ++i) {
    promises.push(awsHelper.putFileToBucket({
      Body: JSON.stringify(trips[i]),
      Bucket: muniConfig.tripBucket,
      Key: `muni-${trips[i].vid}_${trips[i].route}_${trips[i].direction}-${trips[i].startTime}-${trips[i].endTime}.json`
    }))
  }

  return Promise.all(promises).then((results) => {
    for (let i = 0; i < results.length; ++i) {
      console.log("Uploaded state file to s3", results[i].status);
    }

    return results;
  }).catch((err) => {
    console.log("Error writing trip files", err)
  })
}

const transformVehicleToTrip = (vehicle) => {
  const tripStartTime = Date.now()
  return {
    agency: "muni",
    startTime: tripStartTime,
    endTime: null,
    route: vehicle.routeId,
    direction: vehicle.directionId,
    vid: vehicle.id,
    states: [{
      vtime: tripStartTime,
      lat: vehicle.lat,
      lon: vehicle.lon
    }]
  }
}

const getVehicleDataAsTrips = (bucket, key) => {
  return new Promise((resolve, reject) => {
    if (bucket && key) {
      awsHelper.readTextS3(bucket, key).then((result) => {
        const vehicleData = JSON.parse(result)
        console.log("Loading trips from most recent muni trip file at s3://" +
          bucket + "/" + key);
        resolve(vehicleData.map(transformVehicleToTrip));
      }).catch((err) => {
        reject(err)
      });
    }
    else {
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

          const keys = result.sort(sortFn);

          if (keys.length === 0) {
            console.log("Could not find any vehicle data files in bucket s3://" + muniConfig.vehicleBucket)
            resolve([]);
          }

          const mostRecentFile = keys[0].Key;
          awsHelper.readTextS3(muniConfig.vehicleBucket, mostRecentFile).then((result) => {
            const vehicleData = JSON.parse(result)
            console.log("Loading trips from most recent muni trip file at s3://" +
              muniConfig.vehicleBucket + "/" + mostRecentFile);
            resolve(vehicleData.map(transformVehicleToTrip));
          }).catch((err) => {
            reject(err)
          });
        });
    }
  });
}

const writeTripStateFile = (trips) => {
  return awsHelper.putFileToBucket({
    Body: JSON.stringify(trips),
    Bucket: muniConfig.stateBucket,
    Key: muniConfig.stateFile
  })
}

const initTripState = () => {
  return awsHelper.readTextS3(muniConfig.stateBucket, muniConfig.stateFile).then((data) => {
    //first try to load state file
    console.log("Loaded trip state from s3://" + muniConfig.stateBucket + "/" + muniConfig.stateFile)
    return JSON.parse(data);
  }).catch((err) => {
    //next try to get trips from latest muni vehicle data file
    return getVehicleDataAsTrips().then((trips) => {
      //create the state file
      return writeTripStateFile(trips).then((result) => {
        console.log("Uploaded state file to s3", result.status);
        return JSON.parse(result.data);
      }).catch((err) => {
        console.log("Error uploaded state file to s3", err);
      })
    });
  });
}

module.exports = {
  initTripState: initTripState,
  getVehicleDataAsTrips: getVehicleDataAsTrips,
  writeTrips: writeTrips,
  updateTripState: updateTripState,
  writeTripStateFile: writeTripStateFile
}