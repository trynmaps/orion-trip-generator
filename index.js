const muni = require('./agencies/muni/muni');
const sqsConsumer = require('sqs-consumer');
 
const app = sqsConsumer.create({
  queueUrl: 'https://sqs.us-west-2.amazonaws.com/140106082056/new-muni-vehicle-raw-data',
  handleMessage: (message, done) => {
    console.log("handle message")
    let body = JSON.parse(message.Body);
    let messageDetail = JSON.parse(body.Message);
    // console.log(JSON.stringify(messageDetail, null, 2));

    messageDetail.Records.map((record) => {
      let bucket = record.s3.bucket.name;
      let key = record.s3.object.key;

      console.log(bucket);
      console.log(key)
    })

    done();
  }
});

app.trips = [];
app.on('error', (err) => {
  console.log(err.message);
});

muni.initTripState().then((trips) => {
  app.trips = trips;
  //console.log("trips", JSON.stringify(app.trips, null, 2));
  console.log("Loaded", app.trips.length, "trips on startup");  
  app.start();
}).catch((err) => {
  console.log("error starting app", err);
})