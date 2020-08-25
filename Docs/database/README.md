# MongoDB

## Docker Image

Get the latest mobo image from docker hub:

```bash
$ docker pull mongo
```

Create a mongo container for the NLP database. It will listen on port `27000`. This will also run it.

```bash
$ docker run -d -p 27000:27017 --name nlp-mongo mongo:latest
```

Run an existing container (after it was shut down):

```bash
$ start nlp-mongo
```

Dive into the running container to access the mongo shell:

```bash
$ docker exec -it nlp-mongo bash
```

Start the mongo shell:

```bash
$ mongo
```

## Database Commands

The following commands can be run from within the mongo shell or with an editor like Robo3T.

![MongoEditorRobo3T](../images/MongoEditorRobo3T.png)

#### Create Database

Inspect the databases and create the database `nlp`:

```javascript
> show dbs
admin   0.000GB
config  0.000GB
local   0.000GB
> use nlp
switched to db nlp
```

#### Create User

Create a user for our web service which will access the data.

```javascript
db.createUser({
  user: "ScalaWebPlay",
  pwd: "SuperSecretAccessPassword",
  roles: [
    { role: "readWrite", db: "nlp" }
  ]
})
```

Dementsprechend setzen wir im ScalaWebPlay-Pojekt an die `application.conf`: 

```javascript
play.modules.enabled += "play.modules.reactivemongo.ReactiveMongoModule"
mongodb.uri = "mongodb://ScalaWebPlay:SuperSecretAccessPassword@localhost:27000/nlp"
```

#### Create Paragraph Collection

Create collection schema for paragraphs:

```javascript
db.createCollection("paragraphs", {
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["paperId", "paragraphIndex", "text", "indicators"],
      properties: {
        paperId: {
          bsonType: "string",
          description: "kaggle source paper identifier"
        },
        paragraphIndex: {
          bsonType: "int",
          description: "enumerating index of the paragraph in the the paper"
        },
        text: {
          bsonType: "string",
          description: "actual text content of the paragraph"
        },
        indicators: {
          bsonType: "array",
          items: {
            bsonType: "object",
            required: ["word", "begin", "end",
                       "sentenceIndex", "sentenceBegin", "sentenceEnd"],
            description: "tag decription for smoking related text parts",
            properties: {
              word: {
                bsonType: "string",
                description: "word identified as smoking related"
              },
              begin: {
                bsonType: "int",
                description: "start coordinate of the word in the paragraph"
              },
              end: {
                bsonType: "int",
                description: "end coordinate of the word in the paragraph"
              },
              sentenceIndex: {
                bsonType: "int",
                description: "enumerting index of the sentence in the paragraph"
              },
              sentenceBegin: {
                bsonType: "int",
                description: "start coordinate of the sentence in the paragraph"
              },
              sentenceEnd: {
                bsonType: "int",
                description: "end coordinate of the sentence index in the paragraph"
              }
            }
          }
        }
      }
    }
  }
})
```

Insert example paragraph to check schema validation:

```javascript
db.paragraphs.insert({
  "paperId":"0a1abc458accfcceb47f047357f06b4a86fddcbe",
  "paragraphIndex": NumberInt(41),
  "text": "Patients with MERS often present themselves to a hospital with systemic and lower respiratory tract (LRT) signs and symptoms of disease which usually include fever, chills or rigors, dry or productive cough, shortness of breath (dyspnea) and one or more comorbidities including diabetes (prevalent in the KSA), chronic kidney disease including renal failure, chronic heart disease and heart failure, recent surgery, hypertension, chronic lung disease, asthma, obesity, smoking, malignant disease or steroid use (Arabi et al., 2014; Assiri et al., 2013a; Hijawi et al., 2013; Zaki et al., 2012) . MERS-CoV may be identified in patients with severe hypoxaemic respiratory failure and extrapulmonary organ dysfunction which can precede death in over a third of infections (Arabi et al., 2014; Assiri et al., 2013a; Hijawi et al., 2013; Zaki et al., 2012) . Extrapulmonary disease manifestations include circulatory, renal, hepatic and hematologic dysfunction. Gastrointestinal symptoms have been seen in 20-33% of cases (Assiri et al., 2013a; Mailles et al., 2013; Memish et al., 2013b; Zumla and Memish, 2014) , manifesting as diarrhea, vomiting and abdominal pain. Gastrointestinal symptoms were not seen at all in one family cluster (Omrani et al., 2013) nor among symptomatic children in another . On occasion, fever and gastrointestinal upset may form a prodrome, after which symptoms decline to be are later followed by more severe systemic and respiratory signs and symptoms (Kraaij-Dirkzwager et al., 2014; Mailles et al., 2013) . Rarely, MERS-CoV has been detected in a person with fever but no respiratory or gastrointestinal symptoms . The extent to which infection by other gastrointestinal pathogens affect this variability is unknown.",
	"indicators": [
    {
      "word":"smoking",
      "begin": NumberInt(469),
      "end": NumberInt(475),
      "sentenceIndex": NumberInt(0),
      "sentenceBegin": NumberInt(228),
      "sentenceEnd": NumberInt(523)
    }
  ]
})
```

To query for that example paragraph:

```javascript
db.getCollection('paragraphs').find({
  "paperId":"0a1abc458accfcceb47f047357f06b4a86fddcbe",
  "paragraphIndex": 41
})
```

#### Create Sentences Collection

Create collection schema for sentences:

```javascript
db.createCollection("sentences", {
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["paperId", "paragraphIndex", "text",
                 "begin", "end", "similar", "indicators", "pos", "sentimentScore"],
      properties: {
        paperId: {
          bsonType: "string",
          description: "kaggle source paper identifier"
        },
        paragraphIndex: {
          bsonType: "int",
          description: "enumerating index of the paragraph in the the paper"
        },
        sentenceIndex: {
          bsonType: "int",
          description: "enumerating index of the sentence in the the paragraph"
        },
        text: {
          bsonType: "string",
          description: "actual text content of the sentence"
        },
        begin: {
          bsonType: "int",
          description: "start coordinate of the sentence in the paragraph"
        },
        end: {
          bsonType: "int",
          description: "end coordinate of the sentence in the paragraph"
        },
        similar: {
          bsonType: "array",
          items: {
            bsonType: "object",
            required: ["paperId", "paragraphIndex", "sentenceIndex"],
            properties: {
              paperId: {
                bsonType: "string",
                description: "kaggle source paper identifier"
              },
              paragraphIndex: {
                bsonType: "int",
                description: "enumerating index of the paragraph in the the paper"
              },
              sentenceIndex: {
                bsonType: "int",
                description: "enumerating index of the sentence in the the paragraph"
              },
              distance: {
                bsonType: "double",
                description: "similarity distance from sentence vector calculations"
              }
            }
          } 
        },
        indicators: {
          bsonType: "array",
          items: {
            bsonType: "object",
            required: ["word", "begin", "end"],
            description: "tag decription for smoking related words",
            properties: {
              word: {
                bsonType: "string",
                description: "word identified as smoking related"
              },
              begin: {
                bsonType: "int",
                description: "start coordinate of the word in the sentence"
              },
              end: {
                bsonType: "int",
                description: "end coordinate of the word in the sentence"
              }
            }
          }
        },
        pos: {
          bsonType: "array",
          items: {
            bsonType: "object",
            required: ["word", "tag", "begin", "end"],
            description: "tag decription for part of speech",
            properties: {
              word: {
                bsonType: "string",
                description: "word identified as smoking related"
              },
              tag: {
                bsonType: "string",
                description: "short pos identifier tag"
              },
              begin: {
                bsonType: "int",
                description: "start coordinate of the word in the paragraph"
              },
              end: {
                bsonType: "int",
                description: "end coordinate of the word in the paragraph"
              }
            }
          }
        },
        sentimentScore: {
          bsonType: "double",
          description: "a numeric score for sentiments ranking"
        }
      }
    }
  }
})
```

Insert example sentence to check schema validation:

```javascript
db.sentences.insert({
  "paperId":"0a1c4cc123883891af9f8d6b20375098c8efac82",
  "paragraphIndex": NumberInt(4),
  "sentenceIndex": NumberInt(1),
  "text":"Three days after germination, seedlings (4 cm in height) were transferred to pots filled with a sterilized soil/vermiculite (3:1) mixture in a greenhouse at EMBRAPA/ CNPAB, Rio de Janeiro, under conditions described by Loureiro et al.",
  "begin": NumberInt(173),
  "end": NumberInt(406),
  "similar": [],
  "indicators":[
    {
      "word":"pots",
      "begin": NumberInt(77),
      "end": NumberInt(80)
    }
  ],
  "pos":[],
  "sentimentScore": 0.0
})
```

```javascript
db.sentences.insert({
  "paperId":"00a0ab182dc01b6c2e737dfae585f050dcf9a7a5",
  "paragraphIndex": NumberInt(1),
  "sentenceIndex": NumberInt(1),
  "text":"(dyspnea) and one or more comorbidities including diabetes (prevalent in the KSA), chronic kidney disease including renal failure, chronic heart disease and heart failure, recent surgery, hypertension, chronic lung disease, asthma, obesity, smoking, malignant disease or steroid use (Arabi et al.",
  "begin": NumberInt(228),
  "end": NumberInt(523),
  "similar":[],
  "indicators":[{"word":"smoking","begin": NumberInt(241),"end": NumberInt(247)}],
  "pos":[
    {"word":"Patients","tag":"NNS","begin": NumberInt(0),"end": NumberInt(7)},
    {"word":"with","tag":"IN","begin": NumberInt(9),"end": NumberInt(12)},
    {"word":"MERS","tag":"NNP","begin": NumberInt(14),"end": NumberInt(17)}
   ],
  "sentimentScore": 0.0
})
```

To query for that example sentence:

```javascript
db.getCollection('sentences').find({
  "paperId":"0a1c4cc123883891af9f8d6b20375098c8efac82",
  "paragraphIndex": 4,
  "sentenceIndex": 1
})
```

#### Import Data from ScalaNLP

The JSON data from our Spark project can be imported into the Mongo database via the web-application using Postman requests. Use POST requests with the urls listed below. The requests do not directly carry the data as a payload. Instead they just trigger a command which prompts web-application to read in all the json files in the output folders of our ScalaNLP (Spark) project and insert these into the Mongo database. This method removes the need for a mongo interface within the Spark project.

To import the smoking paragraphs data:

```javascript
POST    /smoking/import/paragraphs
```

To import the smoking indicator sentences:

```javascript
POST    /smoking/import/sentences
```

Data import with Postman. Web application is hosted locally on `localhost:9000`.

![](../images/PostmanDataImport.png)

