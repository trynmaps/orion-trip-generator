const muni = require('./agencies/muni/muni');
const sqsConsumer = require('sqs-consumer');

const app = sqsConsumer.create({
  queueUrl: 'https://sqs.us-west-2.amazonaws.com/140106082056/new-muni-vehicle-raw-data',
  handleMessage: (message, done) => {
    const body = JSON.parse(message.Body);
    const messageDetail = JSON.parse(body.Message);

    messageDetail.Records.map((record) => {
      console.log("New vehicle file published in bucket", record.s3.bucket.name, "key", record.s3.object.key)
      muni.getVehicleDataAsTrips(record.s3.bucket.name, record.s3.object.key).then((newTrips) => {
        muni.updateTripState(app.trips, newTrips).then((result) => {
          const newTrips = result[0];
          const updatedTrips = result[1];
          const endedTrips = result[2];
          muni.writeTrips(endedTrips).then((result) => {
            console.log("Wrote", endedTrips.length, "new trips")
            console.log("Write status is", result)
            app.trips = updatedTrips.concat(newTrips);
            muni.writeTripStateFile(app.trips).then((result) => {
              console.log("Uploaded state file to s3", result.status);
            }).catch((err) => {
              console.log("Error uploaded state file to s3", err);
            })
          }).catch((err) => {
            console.log("Error writing new trips", err)
          })
        }).catch((err) => {
          console.log("Error updating trip state", err)
        })
      }).catch((err) => {
        console.log("Error reading new vehicle data from s3://" + record.s3.bucket.name + "/" + record.s3.object.key)
        console.log(err)
      })
    })
    done();
  }
});

app.on('error', (err) => {
  console.log(err.message);
});

muni.initTripState().then((trips) => {
  app.trips = [];
  app.trips = trips;
  console.log("Loaded", app.trips.length, "trips on startup");
  app.start();
}).catch((err) => {
  console.log("error starting app", err);
})