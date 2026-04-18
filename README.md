# Report

Dashboard Streamlit ini sekarang fokus hanya pada sheet laporan komentar:

- Spreadsheet key: `12r5OAZXCea_gATd9EEZu9ZCUVHktnpenoRqMefr-GI0`
- Sumber: `Instagram/Otomasi Buka LD/otomasi_buka_ld.py`
- Worksheet default: index `0`

UI dibuat lebih sederhana dan hanya menampilkan:

- ringkasan total komentar
- akun target teratas
- analisis per postingan dengan detail Apify
- tabel log komentar

Cara menjalankan:

```powershell
streamlit run .\Report\app.py
```

Catatan default:

- `credentials.json` diarahkan ke `Instagram/credentials.json`
- aplikasi tidak membaca sheet post lain
- aplikasi tidak memakai data dari folder `Template`
- detail postingan diambil lewat Apify dengan pola yang mengikuti `Instagram/filtered_post.py`
