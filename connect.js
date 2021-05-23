const oracledb = require("oracledb");
const fs = require("fs");
let connection, result;

async function run() {
  try {
    let rawdata = fs.readFileSync("./movies.json");

    connection = await oracledb.getConnection({
      user: "SYSTEM",
      password: "1234",
      connectString: "localhost/orcl",
    });
    var movies = JSON.parse(rawdata);

    console.log("Bağlantı başarılı." + movies[0]["movietype"]); // objelerin içindeki değerlerin undefined olup olmadığı kontrol/deneme kod satırı.
    
    //MOVIES TABLOSUNA VERİ EKLEME KODU
    for (let i = 0; i < 23539; i++) {
      if (movies[i] !== undefined) {
        if (
          movies[i]["title"] === undefined &&
          movies[i]["year"] === undefined &&
          movies[i]["plot"] === undefined
        ) {
          break;
        } else {
          result =  connection.execute(
              "INSERT INTO MOVIES(movie_id,movie_title,movie_year,movie_plot) VALUES(:abv, :bbv, :cbv, dbv)",
              {
                  abv: i + 1,
                  bbv: movies[i]["title"],
                  cbv: movies[i]["year"],
                  dbv: movies[i]["plot"],
              },
              { autoCommit: true },
              {}
          );

          console.log("Rows İnserted " + (i + 1));
        }
      } else {
        console.log("movie yok");
        break;
      }
    }

    //MOVIE_COUNTRIES TABLOSUNA VERİ EKLEME

    for (let i = 0; i < 23539; i++) {
      for (let j = 0; j < 5; j++) {
        if (movies[i] !== undefined ) {
          if (movies[i]["countries"] === undefined) {
            console.log("countries isimli öge bulunamadı. " + i);
          }
      
           else {
            result =  connection.execute(
              "INSERT INTO MOVIE_COUNTRIES(movie_id,country_name) VALUES(:abv, :bbv)",
              { abv: i + 1, bbv: movies[i]["countries"][j] },
              { autoCommit: true },
              {}
            );

            console.log("Rows İnserted " + (i + 1));
          }
        }
         else {
          console.log("movie yok");
          j = 0;
        }
      }
    }

    //MOVIE_DETAILS TABLOSUNA VERİ EKLEME

    for (let i = 0; i < 23539; i++) {
      for (let j = 0; j < 9; j++) {
        if (movies[i] !== undefined) {
            if(movies[i]["type"] === undefined){
                console.log("movie_type bulunamadı.");
            }
            else if (movies[i]["type"][j] === undefined && movies[i]["type"] === undefined) {
                console.log("movie_type ögesi bulunamadı.");
              }
          if (movies[i]["fullplot"] === undefined) {
            console.log("fullplot ögesi bulunamadı.");
          }
          else if (movies[i]["lastupdated"] === undefined) {
            console.log("lastupdated ögesi bulunamadı.");
          }
        
          else if (movies[i]["runtime"] === undefined) {
            console.log("runtime ögesi bulunamadı.");
          }
          else if (movies[i]["rated"] === undefined) {
            console.log("rated ögesi bulunamadı.");
          } 
        else {
            result =  connection.execute(
              "INSERT INTO MOVIE_DETAILS(movie_id,fullplot,lastupdated,movie_type,runtime,rated) VALUES(:abv, :bbv, :cbv, :dbv, :ebv, :fbv)",
              {
                abv: i + 1,
                bbv: movies[i]["fullplot"],
                cbv: movies[i]["lastupdated"],
                dbv: movies[i]["type"][j],
                ebv: movies[i]["runtime"],
                fbv: movies[i]["rated"],
              },
              { autoCommit: true },
              {}
            );

            console.log("Rows İnserted " + (i + 1));
          }
        } else {
          console.log("movie yok");
        }
      }
    }

    //TOMATOES_VIEWER TABLOSUNA VERİ EKLEME

    for (let i = 0; i < 23539; i++) {
      if (movies[i]["tomatoes"] !== undefined) {
        if (movies[i]["tomatoes"]["viewer"] === undefined) {
          console.log("Viewer ögesi bulunamadı.");
        }else if (
            movies[i]["tomatoes"]["viewer"]["rating"] === undefined
          ) {
            console.log("rating ögesi bulunamadı.");
          } 
         else if (
          movies[i]["tomatoes"]["viewer"]["numReviews"] === undefined
        ) {
          console.log("numReviews ögesi bulunamadı.");
        } else if (movies[i]["tomatoes"]["viewer"]["meter"] === undefined) {
          console.log("meter ögesi bulunamadı.");
        } else {
          result =  connection.execute(
            "INSERT INTO TOMATOES_VIEWER(movie_id,rating,num_reviews,meter) VALUES(:abv, :bbv, :cbv, :dbv)",
            {
              abv: i + 1,
              bbv: movies[i]["tomatoes"]["viewer"]["rating"],
              cbv: movies[i]["tomatoes"]["viewer"]["numReviews"],
              dbv: movies[i]["tomatoes"]["viewer"]["meter"],
            },
            { autoCommit: true },
            {}
          );

          console.log("Rows İnserted " + (i + 1));
        }
      } else {
        console.log("tomatoes viewer yok");
      }
    }

    //MOVIE_IMDB TABLOSUNA VERİ EKLEME KODU

    for (let i = 0; i < 23539; i++) {
      if (movies[i]["imdb"] !== undefined) {
        if (
          movies[i]["imdb"]["rating"] === undefined &&
          movies[i]["imdb"]["votes"] === undefined &&
          movies[i]["imdb"]["id"] === undefined
        ) {
          break;
        } else {
          result =  connection.execute(
            "INSERT INTO MOVIE_IMDB(movie_id,imdb_id,rating,votes) VALUES(:abv, :bbv, :cbv, :dbv)",
            {
              abv: i + 1,
              bbv: movies[i]["imdb"]["id"],
              cbv: movies[i]["imdb"]["rating"],
              dbv: movies[i]["imdb"]["votes"],
            },
            { autoCommit: true },
            {}
          );

          console.log("Rows İnserted " + (i + 1));
        }
      } else {
        console.log("imdb yok");
        break;
      }
    }
    //GENRES TABLOSUNA VERİ EKLEME
    for (let i = 0; i < 23539; i++) {
      let j = 0;
      for (j = 0; j < 9; j++) {
        if (movies[i]["genres"] !== undefined) {
          if (movies[i]["genres"][j] === undefined) {
            j = 9;
            break;
          } else {
            result =  connection.execute(
              "INSERT INTO GENRES(movie_id,gen_title) VALUES(:abv, :bbv)",
              { abv: i + 1, bbv: movies[i]["genres"][j] },
              { autoCommit: true },
              {}
            );

            console.log("Rows İnserted " + (i + 1));
          }
        } else {
          console.log("genres yok");
          break;
        }
      }
    }
    //CASTS TABLOSUNA VERİ EKLEME KODU
    for (let i = 0; i < 23539; i++) {
      let j = 0;
      for (j = 0; j < 9; j++) {
        if (movies[i]["cast"] !== undefined) {
          if (movies[i]["cast"][j] === undefined) {
            j = 9;
            break;
          } else {
            result =  connection.execute(
              "INSERT INTO CASTS(movie_id,cast_name) VALUES(:abv, :bbv)",
              { abv: i + 1, bbv: movies[i]["cast"][j] },
              { autoCommit: true },
              {}
            );

            console.log("Rows İnserted " + (i + 1));
          }
        } else {
          console.log("oyuncu yok");
          break;
        }
      }
    }
    //DIRECTORS TABLOSUNA VERİ EKLEME KODU
    for (let i = 0; i < 23539; i++) {
      let j = 0;
      for (j = 0; j < 9; j++) {
        if (movies[i]["directors"] !== undefined) {
          if (movies[i]["directors"][j] === undefined) {
            j = 9;
            break;
          } else {
            result =  connection.execute(
              "INSERT INTO DIRECTORS(movie_id,dir_name) VALUES(:abv, :bbv)",
              { abv: i + 1, bbv: movies[i]["directors"][j] },
              { autoCommit: true },
              {}
            );

            console.log("Rows İnserted " + (i + 1));
          }
        } else {
          console.log("yönetmen yok");
          break;
        }
      }
    }
    //MOVIE_AWARDS TABLOSUNA VERİ EKLEME KODU
    for (let i = 0; i < 23539; i++) {
      if (movies[i]["awards"] !== undefined) {
        if (
          movies[i]["awards"]["wins"] === undefined &&
          movies[i]["awards"]["nominations"] === undefined &&
          movies[i]["awards"]["text"] === undefined
        ) {
          break;
        } else {
          result =  connection.execute(
            "INSERT INTO MOVIE_AWARDS(movie_id,wins,nominations,text) VALUES(:abv, :bbv, :cbv, :dbv)",
            {
              abv: i + 1,
              bbv: movies[i]["awards"]["wins"],
              cbv: movies[i]["awards"]["nominations"],
              dbv: movies[i]["awards"]["text"],
            },
            { autoCommit: true },
            {}
          );

          console.log("Rows İnserted " + (i + 1));
          if(i == 23538){
            console.log("Veri aktarımı tamamlandı.");
          }
        }
      } else {
        console.log("awards yok");
        break;
      }
    }
  } catch (err) {
    console.log("Başarısız " + err);
  } finally {
    try {
      await connection.close();
    } catch (err) {
      console.log(err);
    }
  }
}

run();
