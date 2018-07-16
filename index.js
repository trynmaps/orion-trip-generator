const muni = require('./agencies/muni/muni');

setInterval(updateTrips, 1000);

function updateTrips() {
  Promise.all([muni()]).then((results) => {
    results.map((result) => { console.log(result) })
  }).catch((err) => {
    console.log(err);
  });
}
