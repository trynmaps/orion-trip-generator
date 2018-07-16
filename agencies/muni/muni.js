const awsHelper = require('../../awsHelper')
const muniConfig = require('./muniConfig');
let trips = []

const updateTrips = () => {
  return new Promise((resolve, reject) => {
    if (trips.length === 0) {
      awsHelper.readTextS3(muniConfig.stateBucket, muniConfig.stateFile).then((data) => {
        //first try to load state file
        console.log("Loaded trip state from s3://" + muniConfig.stateBucket + "/" + muniConfig.stateFile)
        jsonData = JSON.parse(data)
        const trips = jsonData.trips;
        const lastVehicleFileProcessed = jsonData.lastFileProcessed;
        resolve({trips: trips, lastFileProcessed: lastVehicleFileProcessed})
      }).catch((err) => {
        //no trip state file found
        findEarliestVehcileFile().then((result) => {
          resolve({trip: [], lastFileProcessed: result})
        })
      });
    }
    else {
      return 2
    }

    // resolve(trips)
  });
}

const findEarliestVehcileFile = () => {
  return awsHelper.listBucket(
    { Bucket: muniConfig.vehicleBucket, Prefix: muniConfig.agencyKey })
}

const updateTripState = (existingTrips, newTrips, newTripVehicleFileTS) => {
  return new Promise((resolve, reject) => {
    newTrips = newTrips || []

    if (existingTrips.length === 0) {
      resolve([newTrips, [], []]);
    }

    const reducer = (acc, cur) => {
      const index = newTrips.findIndex((n) => { 
        return n.vid === cur.vid && n.route === cur.route && n.direction === cur.direction
      })
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
        cur.endTime = newTripVehicleFileTS
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
    throw err;
  })
}

const transformVehicleToTrip = (vehicle, tripStartTime) => {
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
        let vehicleFileTS = Number(key.split("_")[1].split(".")[0])
        resolve(vehicleData.map(((vehicle) => transformVehicleToTrip(vehicle, vehicleFileTS))));
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
            let vehicleFileTS = Number(mostRecentFile.split("_")[1].split(".")[0])
            resolve(vehicleData.map(((vehicle) => transformVehicleToTrip(vehicle, vehicleFileTS))));
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
    jsonData = JSON.parse(data)
    const trips = jsonData.trips;
    const lastVehicleFileProcessed = jsonData.lastFileProcessed;
    return {trips: trips, lastFileProcessed: lastVehicleFileProcessed}
  }).catch((err) => {
    //no trip state file found
    return {trip: [], lastFileProcessed: null}
  });
}

module.exports = updateTrips