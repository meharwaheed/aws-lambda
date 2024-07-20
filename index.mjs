import { S3Client, GetObjectCommand } from '@aws-sdk/client-s3';
import csv from 'csv-parser';
import { PassThrough } from 'stream';
// import mysqlConn from './connection';
import mysql from 'mysql2/promise';
import { log } from 'console';
import moment from 'moment';


const s3Client = new S3Client();

const dates = [];

export const handler = async (event) => {
    const bucket = event.Records[0].s3.bucket.name;
    const key = event.Records[0].s3.object.key;
    console.log("bucket: " + bucket + " key:" + key)

    try {
        const data = await s3Client.send(new GetObjectCommand({ Bucket: bucket, Key: key }));
        const csvData = await streamToBuffer(data.Body);
        const results = [];
        const connection =  await mysqlConn();
        await connection.beginTransaction();

        return new Promise((resolve, reject) => {
            const bufferStream = new PassThrough();
            bufferStream.end(csvData);

            bufferStream.pipe(csv())
                .on('data', async (row) => {
                    const object = {}
                    Object.keys(row).forEach(key => {
                         object[camelToSnakeCase(key)] = row[key]
                    })
                    // results.push(object);
                    pushArray(object.begin_time)
                    pushArray(object.end_time)
                    await insertMany(object, connection)
                    console.log("Dates", dates)

                })
                .on('end', async () => {
                    await connection.commit();
                    console.log("File Uploaded")
                    await generateStatistics()
                    // console.log(results.length + ' rows created')
                    resolve({
                        statusCode: 200,
                        body: JSON.stringify(results),
                    });
                })
                .on('error', async (error) => {
                    console.er
                    ror('Error processing CSV file', error);
                    await connection.rollback();
                    reject({
                        statusCode: 500,
                        body: JSON.stringify({ message: 'Error processing CSV file', error }),
                    });
                });
        });
    } catch (error) {
        console.error('Error fetching file from S3', error);
        return {
            statusCode: 500,
            body: JSON.stringify({ message: 'Error fetching file from S3', error }),
        };
    }
};

async function streamToBuffer(stream) {
    return new Promise((resolve, reject) => {
        const chunks = [];
        stream.on('data', (chunk) => {
            chunks.push(chunk);
        });
        stream.on('end', () => {
            resolve(Buffer.concat(chunks));
        });
        stream.on('error', (error) => {
            reject(error);
        });
    });
}


const camelToSnakeCase = str => str.replace(/[A-Z]/g, letter => `_${letter.toLowerCase()}`);

const insertData = async (rows) => {
    
    const conn =  await mysqlConn();
    if(conn) {
        // const chunks = chunkArray(rows, 2000)

        // for(let i= 0; i< chunks.length; i++) {
            await insertMany(rows, conn)
        // }

        console.log('File data imported successfully!')

    } else {
        console.log("Unable to import data due to mysql connection issue")
    }



}


const insertMany =  async (data, connection) => {
        console.log("Data Set", data)
 
        // let key = 1;
        // for (const data of dataArray) {
            await connection.execute(`
                INSERT INTO cycle_data 
                (begin_id, begin_time, begin_module_count, end_id, end_time, end_module_count, cycle_duration, success, message, operator_assisted, module, clamp, double_module, temp_ambient, pressure, humidity, wind_speed_peak, gps_acquired, gps_latitude, gps_longitude, avg_soc_lv, avg_soc_hv)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            `, [
                data.begin_id ?? "",
                data.begin_time ?? "",
                data.begin_module_count ?? "",
                data.end_id ?? "",
                data.end_time ?? "",
                data.end_module_count ?? "",
                data.cycle_duration ?? "",
                data.success ?? "",
                data.message ?? "",
                data.operator_assisted ?? "",
                data.module ?? "",
                data.clamp ?? "",
                data.double_module ?? "",
                data.temp_ambient ?? "",
                data.pressure ?? "",
                data.humidity ?? "",
                data.wind_speed_peak ?? "",
                data.gps_acquired ?? "",
                data.gps_latitude ?? "",
                data.gps_longitude ?? "",
                data.avg_soc_lv ?? "",
                data.avg_soc_hv ?? ""
            ]);
// 
            // key ++;
            // console.log("Row inserted #" + key);
        // }
         console.log("row inserted");
}


const chunkArray  = (array, size) => {
    // Ensure the size is a positive integer
    if (size <= 0) throw new Error("Chunk size must be a positive integer");

    // Initialize an empty array to hold the chunks
    const chunkedArray = [];

    // Iterate over the array in steps of the chunk size
    for (let i = 0; i < array.length; i += size) {
        // Slice the array from the current index to the current index + chunk size
        const chunk = array.slice(i, i + size);

        // Add the chunk to the chunkedArray
        chunkedArray.push(chunk);
    }

    // Return the array containing the chunks
    return chunkedArray;
}








const mysqlConn = async () => {
    // Define your MySQL connection parameters
    const connectionConfig = {
        host: "mysql-cluster.chaiu0oigamj.us-east-2.rds.amazonaws.com",
        user: "root",
        password: "4313_786Mysql",
        database: "solar_reports",
    };

    try {

        return await mysql.createConnection(connectionConfig);

    } catch (error) {
        console.error('Error connecting to the database', error);
        return false
    }
};





const dateFormat = (dateTimeString) => {

    // Parse the date using moment.js
    const parsedDate = moment(dateTimeString, 'YYYY-MM-DD HH:mm:ss');

    // Format the date as YYYY-MM-DD
    return parsedDate.format('YYYY-MM-DD');

}

/**
 * 
 * @param {*} date 
 * @returns 
 */
const pushArray = (date) => {
    date = dateFormat(date)

    if(!dates.includes(date)) {
        dates.push(date)
    }

    return true;

}







async function generateStatistics() {
    const connection = await mysqlConn()
    // const dates = [
    //     '2024-07-13', '2024-07-11',
    //     '2024-07-10', '2024-07-09',
    //     '2024-07-02', '2024-07-01',
    //     '2024-06-30', '2024-06-29',
    //     '2024-06-28', '2024-06-27',
    //     '2024-06-26', '2024-06-25'
    // ];

    const result = []

    for (let i = 0; i < dates.length; i++) {
        let row = {date: dates[i]}

        /**
         * Get fastest cycle time
         */
        let [rows, fields] = await connection.execute(
            `SELECT cycle_duration as fastest_cycle_time
             FROM cycle_data
             where date (begin_time) = date ('${dates[i]}')
             ORDER BY TIMESTAMPDIFF(SECOND, begin_time, end_time) ASC limit 1
            `);
        if (rows?.length) {
            row = {...row, ...rows[0]}
        } else {
            row["fastest_cycle_time"] = 0
        }


        /**
         * Get temp_ambient, humidity, total_installed_modules and total_modules
         */

        let response = await connection.execute(`
            SELECT temp_ambient,
                   humidity,
                   sum(begin_module_count - end_module_count) as installed_modules,
                   sum(begin_module_count)                    as total_modules,
                   COUNT(IF(success = 'true', success, NULL)) as total_success_cycles,
                   COUNT(IF(success = 'false', 1, NULL))      as total_fail_cycles,
                   gps_latitude as lat,
                   gps_longitude as lng
            FROM cycle_data
            where date (begin_time) = date ('${dates[i]}')
            GROUP by date (begin_time)
            order by temp_ambient DESC
        `)

        if (response[0].length) {
            row['installed_modules'] = response[0][0]?.installed_modules || 0
            row['total_modules'] = response[0][0]?.total_modules || 0
            row['failed_modules'] = response[0][0]?.total_modules - response[0][0]?.installed_modules || 0
            row['total_success_cycles'] = response[0][0]?.total_success_cycles || 0
            row['total_fail_cycles'] = response[0][0]?.total_fail_cycles || 0
            row['lat'] = response[0][0]?.lat || 0
            row['lng'] = response[0][0]?.lng || 0
            if (parseFloat(response[0][0]?.temp_ambient) > 0 && parseFloat(response[0][0]?.humidity) > 0) {
                row['heat_index'] = calculateHeatIndex(response[0][0].temp_ambient, response[0][0].humidity)
            } else {
                row['heat_index'] = '0°C'
            }
        } else {
            row['heat_index'] = '0°C'
            row['installed_modules'] = 0
            row['total_modules'] = 0
            row['failed_modules'] = 0
            row['total_success_cycles'] = 0
            row['total_fail_cycles'] = 0
            row['lat'] = 0
            row['lng'] = 0
        }

        /**
         * Best one performance hour
         */

        let res = await connection.execute(`
            SELECT DATE_FORMAT(begin_time, '%Y-%m-%d %H:00:00')     AS hour_start,
                   SUM(TIMESTAMPDIFF(SECOND, begin_time, end_time)) AS total_duration_seconds,
                   sum(begin_module_count - end_module_count)       AS max_performance_number
            FROM cycle_data
            where date (begin_time) = date ('${dates[i]}')
            GROUP BY
                hour_start
            ORDER BY
                max_performance_number DESC
                LIMIT 1;
        `)

        if (res[0]?.length) {
            row['best_1_hour_performance'] = res[0][0]?.max_performance_number || 0
            row['best_10_hours_performance'] = Number(res[0][0].max_performance_number) * 10
        } else {
            row['best_1_hour_performance'] = 0
            row['best_10_hours_performance'] = 0
        }


        /**
         * Best three performance hours
         */

        let best3Response = await connection.execute(`
            SELECT DATE_FORMAT(begin_time, '%Y-%m-%d %H:00:00')     AS hour_start,
                   SUM(TIMESTAMPDIFF(SECOND, begin_time, end_time)) AS total_duration_seconds,
                   sum(begin_module_count - end_module_count)       AS max_performance_number
            FROM cycle_data
            where date (begin_time) = date ('${dates[i]}')
            GROUP BY
                hour_start
            ORDER BY
                max_performance_number DESC
                LIMIT 3;
        `)

        row["best_3_hours_performance"] = 0;
        if (best3Response[0]?.length) {
            for (let j = 0; j < best3Response[0].length; j++) {
                row["best_3_hours_performance"] += parseInt(best3Response[0][j]?.max_performance_number || 0)
            }
        }

        result.push(row)
    }



    await createReport(result, connection)
    console.log(result)
}



/**
 *
 * Calculate Heat Index from tempature and humidity
 * @param {*} tempCelsius
 * @param {*} humidity
 * @returns
 */
const calculateHeatIndex = (tempCelsius, humidity) => {
    // Convert temperature from Celsius to Fahrenheit
    const tempFahrenheit = (tempCelsius * 9 / 5) + 32;

    // Constants for the heat index formula
    const c1 = -42.379,
        c2 = 2.04901523,
        c3 = 10.14333127,
        c4 = -0.22475541,
        c5 = -6.83783 * Math.pow(10, -3),
        c6 = -5.481717 * Math.pow(10, -2),
        c7 = 1.22874 * Math.pow(10, -3),
        c8 = 8.5282 * Math.pow(10, -4),
        c9 = -1.99 * Math.pow(10, -6);

    // Calculate heat index in Fahrenheit
    const heatIndexFahrenheit = c1 +
        c2 * tempFahrenheit +
        c3 * humidity +
        c4 * tempFahrenheit * humidity +
        c5 * Math.pow(tempFahrenheit, 2) +
        c6 * Math.pow(humidity, 2) +
        c7 * Math.pow(tempFahrenheit, 2) * humidity +
        c8 * tempFahrenheit * Math.pow(humidity, 2) +
        c9 * Math.pow(tempFahrenheit, 2) * Math.pow(humidity, 2);

    // Convert the heat index back to Celsius
    const heatIndexCelsius = (heatIndexFahrenheit - 32) * 5 / 9;

    return (heatIndexCelsius.toFixed(2) + '°C');
}


const createReport =  async (dataArray, connection) => {
    let key = 1;
    console.log("before start generating report")
    connection.beginTransaction();

    for (const data of dataArray) {
        console.log("report data", data)
    await connection.execute(`
                INSERT INTO cycle_reports 
                (date, fastest_cycle_time, installed_modules, total_modules, failed_modules,
                 total_success_cycles, total_fail_cycles, lat, lng, heat_index, best_1_hour_performance,
                 best_10_hours_performance, best_3_hours_performance)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, [
        data.date ?? "",
        data.fastest_cycle_time ?? "",
        data.installed_modules ?? "",
        data.total_modules ?? "",
        data.failed_modules ?? "",
        data.total_success_cycles ?? "",
        data.total_fail_cycles ?? "",
        data.lat ?? "",
        data.lng ?? "",
        data.heat_index ?? "",
        data.best_1_hour_performance ?? "",
        data.best_10_hours_performance ?? "",
        data.best_3_hours_performance ?? ""
    ]);
//
    key ++;
    console.log("Report Row inserted #" + key);
    }

    connection.commit();
}
