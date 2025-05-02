# Batch Data Processing Using Airflow

## Repository Outline

Penjelasan Mengenai Isi dari Tiap File dan Folder:

1. hafizal_shakur_conceptual.txt - Pertanyaan dan jawaban dari conceptual problems
2. hafizal_shakur_DAG_graph.jpg - Screen capture grafik DAG yang sedang berjalan
3. hafizal_shakur_DAG.py - Program Python yang menjalankan Apache Airflow
4. hafizal_shakur_data_clean.csv - Dataset yang dihasilkan setelah proses data cleaning menggunakan AIrflow
5. hafizal_shakur_data_raw.csv - Dataset original yang akan dimasukkan ke PostGreSQL
6. hafizal_shakur_ddl.txt - Syntax DDL dan DML untuk membuat tabel dan restore data pada PostGreSQL
7. hafizal_shakur_GX.ipynb - Notebook untuk melakukan Data Validation dengan GreatExpectations
8. Images - Folder gambar hasil dari screen capture mengenai grafik dan insight dari KIbana

## Problem Background
Menurut sumber terjadi perubahan pada kebiasaan belanja masyarakat. Dimana masyarakat mulai meninggalkan kegiatan jual beli pada supermarket atau grocery store. Perubahan kebiasaan tersebut mulai dirasakan setelah pandemi Covid-19. Masyarakat lebih memilih untuk berbelanja secara online atau pergi ke minimarket terdekat. Terbukti pada beberapa tahun ini, minimarket yang memiliki nama besar seperti Alfamart dan Indomart meraup untung yang lebih besar jika dibandingkan dengan revenue pada supermarket/grocery store. Selain itu, pada sumber juga menyebutkan faktor lain yang berpengaruh pada daya beli masyarakat di sebuah supermarket adalah kondisi ekonomi.

Oleh sebab itu, Dashboard/Report ini akan dibuat sebagai alat bantu untuk menganalisis strategi apa yang harus dilakukan oleh manajemen ABC Store dalam menghadapi kondisi saat ini. Analisis pada inventoris dari stock barang yang dimiliki ABC Store bertujuan untuk memberikan ide bisnis yang harus dilakukan seperti mendapatkan revenue sebesar-besarnya dan meminimalisir cost. Diantara analisisnya adalah sebagai berikut:

Mengetahui kategori produk pada inventaris ABC Store.
Menampilkan penjualan produk pada kurun waktu satu tahun
Membuat promosi pada item yang paling banyak dibeli masyarakat.
Mengevaluasi kinerja dan harga produk dari supplier


## Project Output

Produk yang dihasilkan dari project ini adalah program Airflow untuk melakukan data cleaning dan data saving, file dataset (csv) yang sudah dibersihkan, dan dashboard Kibana hasil analisis. 

## Data

Dataset merupakan data fiktif yang didapatkan dari website Kaggle. Dataset berisikan daftar inventory produk yang dimiliki oleh sebuah supermarket, terdiri atas 16 column dan 989 rows yang merupakan nama barang pada daftar inventory tersebut. Proses data cleaning dilakukan menggunakan Airflow sehingga missing value dan data duplicated sudah dihilangkan.


## Stacks
`
1. Programming Language : Python, SQL
2. Tools                : Visual Studio Code, Airflow, Elastic Search, Kibana, PgADmin (postgres), Docker, GitHub
3. Library              : pandas, psycopg2, airflow, elasticsearch

## Reference

URL Dataset   : https://www.kaggle.com/datasets/salahuddinahmedshuvo/grocery-inventory-and-sales-dataset/data
---