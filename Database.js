const express = require("express");
const mongoose = require("mongoose");
const https = require("https");

const app = express();

mongoose.connect("mongodb://localhost:27017/netherlands", {
    useNewUrlParser: true,
    useUnifiedTopology: true,
}, (err) => {
    if (err) {
        console.log(err);
    } else {
        console.log("Connected to MongoDB");
        fetchDataAndSaveToMongoDB1();
        fetchDataAndSaveToMongoDB2();
        fetchDataAndSaveToMongoDB3();
    }
});

const problemAreaSchema = new mongoose.Schema({
    apiLink: String,
    values: [{
        Key: String,
        Title: String,
        Description: String,
        CategoryGroupID: String,
    }],
});

const typedDataSetSchema = new mongoose.Schema({
    apiLink: String,
    values: [{
        Key: Number,
        Overlast: String,
        WijkenEnBuurten: String,
        Periode: String,
        GeregistreerdeOverlast_1: Number,
        Gemeentenaam_2: String,
        SoortRegio_3: String
    }],
});

const untypedDataSetSchema = new mongoose.Schema({
    apiLink: String,
    values: [{
        Key: Number,
        Overlast: String,
        WijkenEnBuurten: String,
        Periode: String,
        GeregistreerdeOverlast_1: Number,
        Gemeentenaam_2: String,
        SoortRegio_3: String
    }],
});

const ProblemArea = mongoose.model("ProblemArea", problemAreaSchema);
const TypedDataSet = mongoose.model("TypedDataSet", typedDataSetSchema);
const UntypedDataSet   = mongoose.model("UypedDataSet", untypedDataSetSchema);

function fetchDataAndSaveToMongoDB1() {
    const apiURL1 = "https://dataderden.cbs.nl/ODataApi/OData/47024NED/Overlast";

    https.get(apiURL1, (response) => {
        let data = "";

        response.on("data", (chunk) => {
            data += chunk;
        });

        response.on("end", () => {
            try {
                const apiData = JSON.parse(data);
                const problemArea = new ProblemArea({
                    apiLink: apiURL1,
                    values: apiData.value,
                });

                problemArea.save((err) => {
                    if (err) {
                        console.error("Error saving data to MongoDB:", err);
                    } else {
                        console.log("Data from dataset 1 saved to MongoDB");
                    }
                });
            } catch (error) {
                console.error("Error parsing API response for dataset 1:", error);
            }
        });
    });
}

function fetchDataAndSaveToMongoDB2() {
    const apiURL2 = "https://dataderden.cbs.nl/ODataApi/OData/47024NED/TypedDataSet?$top=5000";

    https.get(apiURL2, (response) => {
        let data = "";

        response.on("data", (chunk) => {
            data += chunk;
        });

        response.on("end", () => {
            try {
                const apiData = JSON.parse(data);
                const typedDataSet = new TypedDataSet({
                    apiLink: apiURL2,
                    values: apiData.value,
                });

                typedDataSet.save((err) => {
                    if (err) {
                        console.error("Error saving data to MongoDB:", err);
                    } else {
                        console.log("Data from dataset 2 saved to MongoDB");
                    }
                });
            } catch (error) {
                console.error("Error parsing API response for dataset 2:", error);
            }
        });
    });
}

function fetchDataAndSaveToMongoDB3() {
    const apiURL3 = "https://dataderden.cbs.nl/ODataApi/OData/47024NED/UntypedDataSet?$top=1000";

    https.get(apiURL3, (response) => {
        let data = "";

        response.on("data", (chunk) => {
            data += chunk;
        });

        response.on("end", () => {
            try {
                const apiData = JSON.parse(data);
                const untypedDataSet = new UntypedDataSet({
                    apiLink: apiURL3,
                    values: apiData.value,
                });

                untypedDataSet.save((err) => {
                    if (err) {
                        console.error("Error saving data to MongoDB:", err);
                    } else {
                        console.log("Data from UntypedDataSet saved to MongoDB");
                    }
                });
            } catch (error) {
                console.error("Error parsing API response for UntypedDataSet:", error);
            }
        });
    });
}

app.listen(3000, () => {
    console.log("Server is running on port 3000");
});
