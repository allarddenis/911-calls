var elasticsearch = require('elasticsearch');
var csv = require('csv-parser');
var fs = require('fs');

var esClient = new elasticsearch.Client({
  host: 'localhost:9200',
  log: 'error'
});

esClient.indices.delete({
  index: '911',
}, (err, resp) => {
  esClient.indices.create({ 
    index: '911',
    body: {
      mappings: {
        call: {
          properties: {
            location: { type: 'geo_point' }
          }
        }
      }
    }
  }, (err, resp) => {
    if (err) console.trace(err.message);
  });
});

let calls = [];

fs.createReadStream('../911.csv')
    .pipe(csv())
    .on('data', data => {
      calls.push({
        title: data.title.split(": ")[1],
        type: data.title.split(": ")[0],
        lat: data.lat,
        lon: data.lng,
        desc: data.desc,
        zip: data.zip,
        date: new Date(data.timeStamp),
        twp: data.twp,
        addr: data.addr
      })
    })
    .on('end', () => {
      esClient.bulk(createBulkInsertQuery(calls), (err, resp) => {
        if (err) console.trace(err.message);
        else console.log(`Inserted ${resp.items.length} calls`);
        esClient.close();
      });
    });

function createBulkInsertQuery(calls) {
  const body = calls.reduce((acc, call) => {
    const { title, type, lat, lon, desc, zip, date, twp, addr } = call;
    acc.push({
      index:
        { _index: '911',
          _type: 'call'
        }
    })
    acc.push({
      title,
      type,
      location: {
        lat,
        lon
      },
      desc,
      zip,
      date,
      twp,
      addr })
    return acc
  }, []);

  return { body };
}