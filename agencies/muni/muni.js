const awsHelper = require('../../awsHelper')
const muniConfig = require('./muniConfig');
let trips = []
let lastVehicleFileProcessed = null

const updateTrips = () => {
  return new Promise((resolve, reject) => {
    if (trips.length === 0) {
      awsHelper.readTextS3(muniConfig.stateBucket, muniConfig.stateFile).then((data) => {
        //first try to load state file
        console.log("Loaded trip state from s3://" + muniConfig.stateBucket + "/" + muniConfig.stateFile)
        jsonData = JSON.parse(data)
        trips = jsonData.trips;
        console.log(jsonData)
        lastVehicleFileProcessed = jsonData.latestVehicleDataFile.file;
        console.log("Loading", trips.length, "trips into memory from state file")
        console.log("Last vehicle file processed was", lastVehicleFileProcessed)
        resolve(trips)
      }).catch((err) => {
        getLatestVehicleFile().then((latestFileKey) => {
          getVehicleDataAsTrips(muniConfig.vehicleBucket, latestFileKey).then((ltrips) => {
            writeTripStateFile(muniConfig.vehicleBucket, latestFileKey, ltrips).then(() => {
              console.log("Wrote", ltrips.length, "trips to state file s3://" + 
                muniConfig.stateBucket + "/" + muniConfig.stateFile)
              trips = ltrips
              lastVehicleFileProcessed = latestFileKey
              resolve(trips)
            }).catch((err) => {
              console.log("Error writing trips to state file", err)
              reject(err)
            })
          }).catch((err) => {
            console.log("Error loading vehicle data from file s3://" + muniConfig.vehicleBucket + "/" + latestFileKey)
            reject(err)
          })
        }).catch((err) => {
          console.log("Error finding latest vehicle file from s3")
          reject(err)
        })
      });
    }
    else {
      readFilesAfter(lastVehicleFileProcessed).then((sortedFiles) => {
        //read each file and update state file after processing it

      })

      resolve("Trips in memory is" + trips.length)
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
    agency: muniConfig.agencyKey,
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

const getLatestVehicleFile = () => {
  return new Promise((resolve, reject) => {
    awsHelper.listBucket(
      { Bucket: muniConfig.vehicleBucket, Prefix: muniConfig.agencyKey }).then((objects) => {
        if (objects.length === 0) {
          resolve(null)
        }
        else {
          const sortFn = (a, b) => {
            const lengthA = a.split("/").length
            const lengthB = b.split("/").length
  
            let tsA = Number(a.split("/")[lengthA - 1].split("_")[1].split(".")[0])
            let tsB = Number(b.split("/")[lengthB - 1].split("_")[1].split(".")[0])
  
            if (tsA > tsB) {
              return -1;
            }
            else {
              return 1;
            }
  
            return 0;
          }
  
          resolve(objects.map(o=>o.Key).filter(o=>o.endsWith(".json")).sort(sortFn)[0])
        }
      });
  })
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
      resolve(null)
    }
  });
}

const writeTripStateFile = (vehicleBucket, latestVehicleFile, trips) => {
  return awsHelper.putFileToBucket({
    Body: JSON.stringify({latestVehicleDataFile: {bucket: vehicleBucket, file: latestVehicleFile}, trips: trips}),
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
    return { trips: trips, lastFileProcessed: lastVehicleFileProcessed }
  }).catch((err) => {
    //no trip state file found
    return { trip: [], lastFileProcessed: null }
  });
}

module.exports = updateTrips