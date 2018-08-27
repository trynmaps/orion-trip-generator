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
        lastVehicleFileProcessed = jsonData.latestVehicleDataFile.file;
        console.log("Loading", trips.length, "trips into memory from state file")
        console.log("Last vehicle file processed was", lastVehicleFileProcessed)
        resolve(trips)
      }).catch((err) => {
        //no state file exists
        getFirstVehicleFile().then((latestFileKey) => {
          getVehicleDataAsTrips(muniConfig.vehicleBucket, latestFileKey).then((mostRecentTrips) => {
            writeTripStateFile(muniConfig.vehicleBucket, latestFileKey, mostRecentTrips).then(() => {
              console.log("Wrote", mostRecentTrips.length, "trips to state file s3://" + 
                muniConfig.stateBucket + "/" + muniConfig.stateFile)
              trips = mostRecentTrips
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
          console.log("Error finding latest vehicle file in s3")
          reject(err)
        })
      });
    }
    else {
      //trips in memory exist
      readFilesAfter(lastVehicleFileProcessed).then(async (sortedFileKeys) => {
        console.log("files to process =", sortedFileKeys)       
        Promise.all(sortedFileKeys.map(async (fileKey) => {
          console.log("Processing file", fileKey)
          const data = await getVehicleDataAsTrips(muniConfig.vehicleBucket, fileKey)

          const length = fileKey.split("/").length
          const timestamp = Number(fileKey.split("/")[length - 1].split("_")[1].split(".")[0])
          return {data: data, timestamp: timestamp, fileKey: fileKey}
        })).then((results) => {
          //results is all new vehicle file data (files after last vehicle file processed) in ascending date order
          results.map((newVehicleFileInfo) => {
            newVehicleInfo = newVehicleFileInfo.data
            newVehicleTimestamp = newVehicleFileInfo.timestamp
            fileKey = newVehicleFileInfo.fileKey

            const reducer = (acc, cur) => {
              const index = newVehicleInfo.findIndex((n) => {
                  return n.vid === cur.vid && n.route === cur.route && n.direction === cur.direction
              })

              //should never be more than one match              
              if (index != -1) {
                //an existing trips matched a trip in the
                //new vehicle file being processed
                const matchingTrip = newVehicleInfo[index]

                let updatedExistingTrip = {
                  agency: matchingTrip.agency,
                  startTime: matchingTrip.startTime,
                  endTime: matchingTrip.endTime,
                  vid: matchingTrip.vid,
                  route: matchingTrip.route,
                  direction: matchingTrip.direction,
                  states: cur.states
                }

                matchingTrip.states.map((matchingTripState) => {
                  updatedExistingTrip.states.push({
                    vtime: matchingTripState.vtime,
                    lat: matchingTripState.lat,
                    lon: matchingTripState.lon
                  })                  
                })
                
                acc.existingTrips.push(updatedExistingTrip)
              }
              else {
                //trip in memory has ended (has no match
                // in the current vehcile file being processed)
                acc.endedTrips.push({
                  agency: cur.agency,
                  startTime: cur.startTime,
                  endtime: cur.endTime,
                  vid: cur.vid,
                  route: cur.route,
                  direction: cur.direction,
                  endTime: newVehicleTimestamp,
                  states: cur.states
                });
              }
        
              return acc;
            };

            const state = trips.reduce(reducer, { existingTrips: [], endedTrips: [] })
            const newTrips = newVehicleInfo.filter((newVehicle) => {
              return trips.map((currentTrip) => {
                newVehicle.vid === currentTrip.vid && 
                  newVehicle.route === currentTrip.route && 
                    newVehicle.direction === currentTrip.direction
              }).length == 0
            })

            //write new trips and updated state file
            //writeTrips(state.endedTrips)
            console.log("ENDED TRIPS:", JSON.stringify(state.endedTrips))
            //update state to reflect the file that was processed
            trips = state.existingTrips.concat(newTrips)
            //writeTripStateFile(muniConfig.stateBucket, fileKey, trips)

            console.log("Processed vehicle file s3://" + 
              muniConfig.vehicleBucket + fileKey + " updating", state.existingTrips.length,
               "trips and writing", state.endedTrips.length, "new trips")

            return "Updated XX trips"
          })
        })
        resolve("Processed " + sortedFileKeys.length + " vehicle files from s3 bucket " + muniConfig.vehicleBucket)
      }).catch((err) => {
        console.log(err)
      })
    }
 });
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

const readFilesAfter = (fileKey) => {
  return new Promise((resolve, reject) => {
    const length = fileKey.split("/").length
    const timestamp = Number(fileKey.split("/")[length - 1].split("_")[1].split(".")[0])
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
  
            //asc order
            if (tsA < tsB) {
              return -1;
            }
            else {
              return 1;
            }
  
            return 0;
          }
  
          resolve(objects.map(o=>o.Key).filter(o=>o.endsWith(".json")).sort(sortFn).filter(
            (thisFileKey) => {
              const l = thisFileKey.split("/").length
              const thists = Number(thisFileKey.split("/")[l - 1].split("_")[1].split(".")[0])
              return (thists > timestamp)              
            }
          ))
        }
      });
  })
}

const writeTrips = (trips) => {
  let promises = [];

  trips.map((trip) => {
    promises.push(awsHelper.putFileToBucket({
      Body: JSON.stringify(trip),
      Bucket: muniConfig.tripBucket,
      Key: `muni-${trip.vid}_${trip.route}_${trip.direction}-${trip.startTime}-${trip.endTime}.json`
    }))    
  })

  return Promise.all(promises).then((results) => {
    results.map((result) => {
      console.log("Uploaded new trip file to s3", result.status);
    })

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

const getFirstVehicleFile = () => {
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
              return 1;
            }
            else {
              return -1;
            }
  
            return 0;
          }
  
          resolve(objects.map(o=>o.Key).filter(o=>o.endsWith(".json")).sort(sortFn)[0])
        }
      });
  })
}

const getVehicleDataAsTrips = async (bucket, key) => {
  return new Promise((resolve, reject) => {
    if (bucket && key) {
      awsHelper.readTextS3(bucket, key).then((result) => {
        const vehicleData = JSON.parse(result)
        const vehicleFileTS = Number(key.split("_")[1].split(".")[0])
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