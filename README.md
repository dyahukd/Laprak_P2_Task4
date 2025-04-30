# Task 4 : Pipip's Load Balancer

## **Deskripsi Soal**
Pipip, seorang pengembang perangkat lunak yang tengah mengerjakan proyek distribusi pesan dengan sistem load balancing, memutuskan untuk merancang sebuah sistem yang memungkinkan pesan dari client bisa disalurkan secara efisien ke beberapa worker. Dengan menggunakan komunikasi antar-proses (IPC), Pipip ingin memastikan bahwa proses pengiriman pesan berjalan mulus dan terorganisir dengan baik, melalui sistem log yang tercatat dengan rapi.

### **a. Client Mengirimkan Pesan ke Load Balancer**

Pipip ingin agar proses `client.c` dapat mengirimkan pesan ke `loadbalancer.c` menggunakan IPC dengan metode **shared memory**. Proses pengiriman pesan dilakukan dengan format input dari pengguna sebagai berikut:

```
Halo A;10
```

**Penjelasan:**

- `"Halo A"` adalah isi pesan yang akan dikirim.
- `10` adalah jumlah pesan yang ingin dikirim, dalam hal ini sebanyak 10 kali pesan yang sama.

Selain itu, setiap kali pesan dikirim, proses `client.c` harus menuliskan aktivitasnya ke dalam **`sistem.log`** dengan format:

```
Message from client: <isi pesan>
Message count: <jumlah pesan>
```

Semua pesan yang dikirimkan dari client akan diteruskan ke `loadbalancer.c` untuk diproses lebih lanjut.

### **b. Load Balancer Mendistribusikan Pesan ke Worker Secara Round-Robin**

Setelah menerima pesan dari client, tugas `loadbalancer.c` adalah mendistribusikan pesan-pesan tersebut ke beberapa **worker** menggunakan metode **round-robin**. Sebelum mendistribusikan pesan, `loadbalancer.c` terlebih dahulu mencatat informasi ke dalam **`sistem.log`** dengan format:

```
Received at lb: <isi pesan> (#message <indeks pesan>)
```

Contoh jika ada 10 pesan yang dikirimkan, maka output log yang dihasilkan adalah:

```
Received at lb: Halo A (#message 1)
Received at lb: Halo A (#message 2)
...
Received at lb: Halo A (#message 10)
```

Setelah itu, `loadbalancer.c` akan meneruskan pesan-pesan tersebut ke **n worker** secara bergiliran (round-robin), menggunakan **IPC message queue**. Berikut adalah contoh distribusi jika jumlah worker adalah 3:

- Pesan 1 → worker1
- Pesan 2 → worker2
- Pesan 3 → worker3
- Pesan 4 → worker1 (diulang dari awal)

Dan seterusnya.

Proses `worker.c` bertugas untuk mengeksekusi pesan yang diterima dan mencatat log ke dalam file yang sama, yakni **`sistem.log`**.

### **c. Worker Mencatat Pesan yang Diterima**

Setiap worker yang menerima pesan dari `loadbalancer.c` harus mencatat pesan yang diterima ke dalam **`sistem.log`** dengan format log sebagai berikut:

```
WorkerX: message received
```

### **d. Catat Total Pesan yang Diterima Setiap Worker di Akhir Eksekusi**

Setelah proses selesai (semua pesan sudah diproses), setiap worker akan mencatat jumlah total pesan yang mereka terima ke bagian akhir file **`sistem.log`**.

```
Worker 1: 3 messages
Worker 2: 4 messages
Worker 3: 3 messages
```

**Penjelasan:**
3 + 4 + 3 = 10, sesuai dengan jumlah pesan yang dikirim pada soal a

## **Kode Program**
### **client.c**
```
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/types.h>
#include <unistd.h>
#include <fcntl.h>
#include <time.h>

typedef struct {
    char message[100];
    int count;
} MessageData;

void log_initial_message(const char* message, int count) {
    FILE* log_file = fopen("sistem.log", "a");
    if (log_file == NULL) {
        perror("Failed to open log file");
        return;
    }
    
    time_t now;
    time(&now);
    fprintf(log_file, "[%.19s] ===== NEW BATCH =====\n", ctime(&now));
    fprintf(log_file, "[%.19s] Message from client: %s\n", ctime(&now), message);
    fprintf(log_file, "[%.19s] Message count: %d\n", ctime(&now), count);
    fclose(log_file);
}

void log_message(const char* message, int count) {
    FILE* log_file = fopen("sistem.log", "a");
    if (log_file == NULL) {
        perror("Failed to open log file");
        return;
    }
    
    time_t now;
    time(&now);
    fprintf(log_file, "[%.19s] Message from client: %s\n", ctime(&now), message);
    fprintf(log_file, "[%.19s] Message count: %d\n", ctime(&now), count);
    
    fclose(log_file);
}

int main() {
    key_t key = ftok("shmfile", 65);
    int shmid = shmget(key, sizeof(MessageData), 0666|IPC_CREAT);
    MessageData *data = (MessageData*) shmat(shmid, (void*)0, 0);
    
    char input[100];
    printf("Enter message and count (format: <message>;<count>): ");
    fgets(input, sizeof(input), stdin);
    
    // Parse input
    char* message = strtok(input, ";");
    char* count_str = strtok(NULL, ";");
    int count = atoi(count_str);
    
    strcpy(data->message, message);
    data->count = count;
    
    // Log to file
    log_message(message, count);
    
    shmdt(data);
    return 0;
}
````

### **loadbalancer.c**
```
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/msg.h>
#include <unistd.h>
#include <fcntl.h>
#include <time.h>

typedef struct {
    char message[100];
    int index;
} WorkerMessage;

typedef struct {
    long mtype;
    WorkerMessage wmsg;
} msgbuf;

typedef struct {
    char message[100];
    int count;
} MessageData;

void log_received(const char* message, int index) {
    FILE* log_file = fopen("sistem.log", "a");
    if (log_file == NULL) {
        perror("Failed to open log file");
        return;
    }
    
    time_t now;
    time(&now);
    fprintf(log_file, "[%.19s] Received at lb: %s (#message %d)\n", ctime(&now), message, index);
    
    fclose(log_file);
}

int main(int argc, char* argv[]) {
    if (argc != 2) {
        printf("Usage: %s <number_of_workers>\n", argv[0]);
        return 1;
    }
    
    int n_workers = atoi(argv[1]);
    if (n_workers <= 0) {
        printf("Number of workers must be positive\n");
        return 1;
    }
    
    // Setup message queues for workers
    int worker_queues[n_workers];
    for (int i = 0; i < n_workers; i++) {
        key_t key = ftok("worker", i+1);
        worker_queues[i] = msgget(key, 0666 | IPC_CREAT);
    }
    
    // Get shared memory from client
    key_t shm_key = ftok("shmfile", 65);
    int shmid = shmget(shm_key, sizeof(MessageData), 0666|IPC_CREAT);
    MessageData *data = (MessageData*) shmat(shmid, (void*)0, 0);
    
    // Distribute messages using round-robin
    for (int i = 0; i < data->count; i++) {
        log_received(data->message, i+1);
        
        msgbuf msg;
        msg.mtype = 1;
        strcpy(msg.wmsg.message, data->message);
        msg.wmsg.index = i+1;
        
        int target_worker = i % n_workers;
        msgsnd(worker_queues[target_worker], &msg, sizeof(msg.wmsg), 0);
    }
    
    // Send termination messages to workers
    for (int i = 0; i < n_workers; i++) {
    msgbuf term_msg;
    term_msg.mtype = 1;
    strcpy(term_msg.wmsg.message, "TERMINATE");
    term_msg.wmsg.index = -1;  // Flag terminasi
    
    if (msgsnd(worker_queues[i], &term_msg, sizeof(term_msg.wmsg), 0) == -1) {
        perror("Failed to send termination signal");
    } else {
        printf("Sent termination to worker %d\n", i+1);
    }
}
    
    shmdt(data);
    shmctl(shmid, IPC_RMID, NULL);
    
    return 0;
}
```

### **worker.c**
```
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ipc.h>
#include <sys/msg.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <time.h>

typedef struct {
    char message[100];
    int index;
} WorkerMessage;

typedef struct {
    long mtype;
    WorkerMessage wmsg;
} msgbuf;

void log_activity(const char *activity) {
    FILE* log_file = fopen("sistem.log", "a");
    if (log_file == NULL) {
        perror("Failed to open log file");
        return;
    }
    
    time_t now;
    time(&now);
    fprintf(log_file, "[%.19s] %s\n", ctime(&now), activity);
    fclose(log_file);
}

int main(int argc, char* argv[]) {
    if (argc != 2) {
        printf("Usage: %s <worker_id>\n", argv[0]);
        return 1;
    }
    
    int worker_id = atoi(argv[1]);
    int message_count = 0;
    char log_buffer[100];

    // Setup message queue
    key_t key = ftok("worker", worker_id);
    if (key == -1) {
        perror("ftok failed");
        exit(1);
    }

    int msgid = msgget(key, 0666 | IPC_CREAT);
    if (msgid == -1) {
        perror("msgget failed");
        exit(1);
    }

    // Main worker loop
    while (1) {
        msgbuf msg;
        
        // Blocking receive
        if (msgrcv(msgid, &msg, sizeof(msg.wmsg), 1, 0) == -1) {
            perror("msgrcv failed");
            break;
        }

        // Check for termination signal
        if (strcmp(msg.wmsg.message, "TERMINATE") == 0 && msg.wmsg.index == -1) {
            break;
        }

        // Log received message
        sprintf(log_buffer, "Worker%d: message received", worker_id);
        log_activity(log_buffer);
        message_count++;
    }

    // Log total messages before exiting
    sprintf(log_buffer, "Worker x%d: %d messages", worker_id, message_count);
    log_activity(log_buffer);

    printf("Worker %d processed %d messages\n", worker_id, message_count);
    return 0;
}
```

## **Penjelasan Kode Program**
### **client.c**
#### **Import Library**
```
#include <stdio.h>      // Digunakan untuk operasi input dan output, seperti printf, fopen, fgets
#include <stdlib.h>     // Digunakan untuk fungsi umum seperti atoi() dan exit()
#include <string.h>     // Digunakan untuk manipulasi string, seperti strtok(), strcpy()
#include <sys/ipc.h>    // Digunakan untuk menghasilkan dan menggunakan key (kunci) IPC (Inter Process Communication)
#include <sys/shm.h>    // Digunakan untuk fungsi shared memory seperti shmget(), shmat(), shmdt()
#include <sys/types.h>  // Digunakan untuk tipe data standar sistem seperti key_t
#include <unistd.h>     // Digunakan untuk fungsi sistem seperti fork(), exec() (tidak dipakai langsung di sini)
#include <fcntl.h>      // Untuk kontrol file descriptor (tidak dipakai di kode ini)
#include <time.h>       // Digunakan untuk mendapatkan waktu saat ini dengan time() dan ctime()
```

#### **Struktur Data**
```
typedef struct {
    char message[100];  // Array karakter untuk menyimpan pesan (maksimal 100 karakter)
    int count;          // Menyimpan jumlah atau jumlah pengulangan dari pesan
} MessageData;
```
Struktur ini akan disimpan dalam shared memory, berfungsi sebagai media berbagi data antar proses.

#### **Fungsi `log_initial_message`**
```
void log_initial_message(const char* message, int count) {
    FILE* log_file = fopen("sistem.log", "a");
```
- Membuka file `sistem.log` dalam mode *append* (`"a"`), artinya data baru akan ditambahkan ke akhir file tanpa menghapus data lama.
- `log_file` adalah pointer ke file log.

```
    if (log_file == NULL) {
        perror("Failed to open log file");
        return;
    }
```
- Mengecek apakah file gagal dibuka. Jika gagal, mencetak pesan kesalahan menggunakan `perror()` dan keluar dari fungsi.

```
    time_t now;
    time(&now);
```
- `now` adalah variabel bertipe `time_t` untuk menyimpan waktu sekarang.
- `time(&now)` mengisi `now` dengan waktu saat ini.

```
    fprintf(log_file, "[%.19s] ===== NEW BATCH =====\n", ctime(&now));
```
- Menuliskan timestamp awal ke file dengan format waktu.
- `ctime(&now)` mengubah waktu menjadi string.
- `%.19s` membatasi output waktu hanya 19 karakter pertama (biasanya: "Day Mon DD HH:MM:SS").

```
    fprintf(log_file, "[%.19s] Message from client: %s\n", ctime(&now), message);
    fprintf(log_file, "[%.19s] Message count: %d\n", ctime(&now), count);
```
- Menuliskan pesan dan jumlahnya ke file log.

```
    fclose(log_file);
```
- Menutup file `log_file` agar tidak terjadi kebocoran file descriptor dan memastikan semua data ditulis.

#### **Fungsi `log_message`**
```
void log_message(const char* message, int count) {
    FILE* log_file = fopen("sistem.log", "a");
```
- Sama seperti sebelumnya, membuka file log untuk menulis di akhir file.

```
    if (log_file == NULL) {
        perror("Failed to open log file");
        return;
    }
```
- Jika file gagal dibuka, cetak error dan keluar dari fungsi.

```
    time_t now;
    time(&now);
```
- Ambil waktu saat ini.

```
    fprintf(log_file, "[%.19s] Message from client: %s\n", ctime(&now), message);
    fprintf(log_file, "[%.19s] Message count: %d\n", ctime(&now), count);
```
- Tulis log berupa pesan dan jumlah ke dalam file.

```
    fclose(log_file);
}
```
- Tutup file setelah selesai menulis.

#### **Fungsi Utama: `main`**
```
int main() {
```
- Titik masuk utama program.

```
    key_t key = ftok("shmfile", 65);
```
- Menghasilkan *key* unik menggunakan file `shmfile` dan angka proyek `65`.

```
    int shmid = shmget(key, sizeof(MessageData), 0666|IPC_CREAT);
```
- Membuat/ambil shared memory dengan ukuran sebesar `MessageData`.
- `0666` adalah permission untuk read dan write bagi semua user.
- `IPC_CREAT` akan membuat segmen jika belum ada.

```
    MessageData *data = (MessageData*) shmat(shmid, (void*)0, 0);
```
- *Attach* (menyambungkan) shared memory ke alamat memori proses saat ini.
- Pointer `data` digunakan untuk mengakses isi shared memory.

```
    char input[100];
    printf("Enter message and count (format: <message>;<count>): ");
    fgets(input, sizeof(input), stdin);
```
- Menyediakan buffer input dari user dan membacanya dari stdin (keyboard).
- Format input diharapkan seperti: `Hello;3`

```
    char* message = strtok(input, ";");
    char* count_str = strtok(NULL, ";");
```
- Memisahkan string input berdasarkan karakter `;`
- `message` akan berisi teks sebelum `;`, sedangkan `count_str` setelahnya.

```
    int count = atoi(count_str);
```
- Mengubah string `count_str` menjadi bilangan bulat (`int`).

```
    strcpy(data->message, message);
    data->count = count;
```
- Menyalin data ke dalam shared memory agar bisa dibaca oleh proses lain.

```
    log_message(message, count);
```
- Memanggil fungsi untuk mencatat pesan ke dalam file log.

```
    shmdt(data);
```
- *Detach* shared memory dari proses setelah selesai digunakan.

```
    return 0;
}
```
- Program selesai dan mengembalikan nilai `0` (berarti sukses).

### **loadbalancer.c**
#### **Import Library**
```
#include <stdio.h>      // Untuk fungsi input/output seperti printf, fopen, fprintf
#include <stdlib.h>     // Untuk fungsi utility seperti atoi, malloc, exit
#include <string.h>     // Untuk manipulasi string seperti strcpy, strtok
#include <sys/ipc.h>    // Untuk fungsi key_t, ftok (membuat key IPC)
#include <sys/shm.h>    // Untuk fungsi shared memory: shmget, shmat, shmctl
#include <sys/msg.h>    // Untuk fungsi message queue: msgget, msgsnd, msgrcv
#include <unistd.h>     // Untuk fungsi POSIX seperti fork, sleep, close
#include <fcntl.h>      // Untuk manipulasi file descriptor (opsional disini)
#include <time.h>       // Untuk mencatat waktu log dengan fungsi time dan ctime
```

#### **Struktur Data**
```
typedef struct {
    char message[100];
    int index;
} WorkerMessage;
```
- Struktur `WorkerMessage` berisi pesan dan indeks urutan.

```
typedef struct {
    long mtype;
    WorkerMessage wmsg;
} msgbuf;
```
- Struktur `msgbuf` adalah format pesan untuk message queue.

```
typedef struct {
    char message[100];
    int count;
} MessageData;
```
- Struktur `MessageData` digunakan dalam shared memory. Menyimpan pesan dari client dan jumlah pesan yang akan dikirim.

#### **Fungsi Logging**
```
void log_received(const char* message, int index) {
    FILE* log_file = fopen("sistem.log", "a");
```
- Membuka file log untuk ditambahkan (`a` = append).

```
    if (log_file == NULL) {
        perror("Failed to open log file");
        return;
    }
```
- Jika gagal membuka file, cetak error dan keluar dari fungsi.

```
    time_t now;
    time(&now);
```
- Mendapatkan waktu saat ini untuk dicatat dalam log.

```
    fprintf(log_file, "[%.19s] Received at lb: %s (#message %d)\n", ctime(&now), message, index);
```
- Menuliskan log pesan dan urutannya.

```
    fclose(log_file);
```
- Menutup file log.

#### **Fungsi `main`**
```
int main(int argc, char* argv[]) {
```
- Fungsi utama program. `argc` = jumlah argumen, `argv[]` = array argumen.

```
    if (argc != 2) {
        printf("Usage: %s <number_of_workers>\n", argv[0]);
        return 1;
    }
```
- Memastikan argumen input benar (jumlah worker). Jika tidak, tampilkan petunjuk.

```
    int n_workers = atoi(argv[1]);
```
- Mengubah argumen string ke integer.

```
    if (n_workers <= 0) {
        printf("Number of workers must be positive\n");
        return 1;
    }
```
- Jika `n_workers` kurang dari sama dengan 0, maka akan mencetak pesan error.

#### **Setup Message Queue**
```
    int worker_queues[n_workers];
    for (int i = 0; i < n_workers; i++) {
        key_t key = ftok("worker", i+1);
        worker_queues[i] = msgget(key, 0666 | IPC_CREAT);
    }
```
- Membuat message queue untuk setiap worker menggunakan `msgget` dan key unik dari `ftok`.

#### **Ambil Shared Memory**
```
    key_t shm_key = ftok("shmfile", 65);
    int shmid = shmget(shm_key, sizeof(MessageData), 0666|IPC_CREAT);
    MessageData *data = (MessageData*) shmat(shmid, (void*)0, 0);
```
- Mengakses shared memory yang dibuat client. Pointer `data` menunjuk ke data yang diterima.

#### **Kirim Pesan ke Worker**
```
    for (int i = 0; i < data->count; i++) {
        log_received(data->message, i+1);
        
        msgbuf msg;
        msg.mtype = 1;
        strcpy(msg.wmsg.message, data->message);
        msg.wmsg.index = i+1;
        
        int target_worker = i % n_workers;
        msgsnd(worker_queues[target_worker], &msg, sizeof(msg.wmsg), 0);
    }
```
- Melakukan iterasi sesuai jumlah `data->count`
- Setiap pesan dikirim ke worker secara bergiliran menggunakan modulo (`i % n_workers`)

#### **Kirim Sinyal Terminasi ke Semua Worker**
```
    for (int i = 0; i < n_workers; i++) {
        msgbuf term_msg;
        term_msg.mtype = 1;
        strcpy(term_msg.wmsg.message, "TERMINATE");
        term_msg.wmsg.index = -1;
```
- Membuat pesan terminasi bertipe `"TERMINATE"` dan indeks `-1` sebagai flag terminasi.

```
        if (msgsnd(worker_queues[i], &term_msg, sizeof(term_msg.wmsg), 0) == -1) {
            perror("Failed to send termination signal");
        } else {
            printf("Sent termination to worker %d\n", i+1);
        }
    }
```
- Mengirim pesan terminasi "TERMINATE" ke tiap antrean worker menggunakan `msgsnd()`. Jika pengiriman gagal, akan dicetak pesan error; jika berhasil, akan ditampilkan notifikasi bahwa terminasi berhasil dikirim ke worker yang bersangkutan.

#### **Tutup Shared Memory**
```
    shmdt(data);
    shmctl(shmid, IPC_RMID, NULL);
```
- `shmdt` = detach memory
- `shmctl(..., IPC_RMID, ...)` = hapus shared memory

```
    return 0;
}
```
- Program selesai.

### **worker.c**
#### **Import Library**
```
#include <stdio.h>       // Untuk fungsi input/output standar seperti printf(), fopen(), dll
#include <stdlib.h>      // Untuk fungsi umum seperti atoi(), exit(), dll
#include <string.h>      // Untuk fungsi manipulasi string seperti strcmp(), strcpy()
#include <sys/ipc.h>     // Untuk membuat key IPC (Inter Process Communication)
#include <sys/msg.h>     // Untuk fungsi message queue seperti msgget(), msgrcv(), msgsnd()
#include <unistd.h>      // Untuk fungsi POSIX seperti sleep(), fork() (meskipun tidak digunakan di sini)
#include <errno.h>       // Untuk menangani dan mencetak kesalahan sistem
#include <fcntl.h>       // Untuk manipulasi file descriptor (tidak digunakan eksplisit di kode ini)
#include <time.h>        // Untuk mencatat waktu ke dalam log
```

#### **Struktur Data**
```
typedef struct {
    char message[100];
    int index;
} WorkerMessage;
```
- Struktur data `WorkerMessage` ini digunakan untuk menyimpan isi pesan yang dikirim oleh load balancer.
- `message`: Isi pesan (misal: pesan teks).
- `index`: Nomor urut pesan.

```
typedef struct {
    long mtype;
    WorkerMessage wmsg;
} msgbuf;
```
- Struktur Data `msgbuf` ini digunakan sebagai format untuk antrean pesan.
- `mtype`: Tipe pesan, harus bertipe `long`.
- `wmsg`: Isi pesan dari tipe `WorkerMessage`.

#### **Fungsi `log_activity`**
```
void log_activity(const char *activity) {
    FILE* log_file = fopen("sistem.log", "a"); // Membuka file log dengan mode append ("a")
    if (log_file == NULL) {
        perror("Failed to open log file");     // Jika gagal membuka file, tampilkan error
        return;
    }

    time_t now;
    time(&now);                                // Mendapatkan waktu saat ini
    fprintf(log_file, "[%.19s] %s\n", ctime(&now), activity); // Menulis waktu dan aktivitas ke file log
    fclose(log_file);                          // Menutup file log setelah selesai
```
- Fungsi ini mencatat semua aktivitas penting (misalnya menerima pesan atau selesai bekerja) ke dalam file log bernama `sistem.log`.

#### **Main Program**
```
int main(int argc, char* argv[]) {
```
- Fungsi utama program.
- `argc`: Jumlah argumen.
- `argv`: Array dari argumen.

1. Validasi Argumen
```
if (argc != 2) {
    printf("Usage: %s <worker_id>\n", argv[0]);
    return 1;
}
```
- Mengecek apakah jumlah argumen sesuai. Jika tidak, tampilkan cara pakai program dan keluar.

2. Inisialisasi Variabel
```
int worker_id = atoi(argv[1]);        // Mengubah argumen worker_id dari string ke integer
int message_count = 0;                // Menghitung jumlah pesan yang diproses oleh worker ini
char log_buffer[100];                 // Buffer untuk menyimpan pesan log sementara
```

3. Setup Message Queue
```
key_t key = ftok("worker", worker_id); // Membuat key unik berdasarkan nama file dan ID worker
if (key == -1) {
    perror("ftok failed");             // Gagal membuat key
    exit(1);
}
```
- Jika gagal membuat key, maka akan mencetak pesan error.

```
int msgid = msgget(key, 0666 | IPC_CREAT); // IPC_CREAT : Membuat atau mendapatkan antrean pesan
if (msgid == -1) {
    perror("msgget failed");               // Gagal membuat/get queue
    exit(1);
}
```
- Jika tidak ada `msgid`, maka akan mencetak pesan error.

4. Loop Utama Worker
```
while (1) {
    msgbuf msg;
```
- Worker akan terus menerima pesan hingga menerima sinyal `"TERMINATE"`.

```
    if (msgrcv(msgid, &msg, sizeof(msg.wmsg), 1, 0) == -1) {
        perror("msgrcv failed");        // Gagal menerima pesan
        break;
    }
```
- Jika tidak ada pesan yang diterima, akan mencetak pesan error.
c
Copy
Edit
    if (strcmp(msg.wmsg.message, "TERMINATE") == 0 && msg.wmsg.index == -1) {
        break;                          // Jika pesan TERMINATE diterima, keluar dari loop
    }
c
Copy
Edit
    sprintf(log_buffer, "Worker%d: message received", worker_id); // Buat pesan log
    log_activity(log_buffer);                                     // Simpan ke file log
    message_count++;                                              // Tambah jumlah pesan
}
5. Setelah Keluar dari Loop (Worker Selesai)
```
sprintf(log_buffer, "Worker x%d: %d messages", worker_id, message_count);
log_activity(log_buffer);
```
- Mencatat jumlah total pesan yang diterima worker ini

```
printf("Worker %d processed %d messages\n", worker_id, message_count);
return 0;
```
- Menampilkan info total pesan yang diproses worker ini ke terminal.

## **Hasil Program**
1. Program `client.c` meminta input berupa pesan dan jumlah pengiriman (format: `<pesan>;<jumlah>`), lalu menyimpan data tersebut ke memori bersama (`shared memory`) serta mencatat log-nya ke `sistem.log`.
2. Program `loadbalancer.c` membaca data dari memori bersama, lalu mengirimkan pesan tersebut secara bergilir ke antrean pesan tiap worker, dan mencatat setiap pengiriman ke `sistem.log`. Setelah semua pesan dikirim, program juga mengirimkan sinyal terminasi ke semua worker.
3. Program `worker.c` menerima pesan dari antrean masing-masing secara terus-menerus. Setiap kali pesan diterima, worker mencatat aktivitasnya di `sistem.log`. Jika menerima pesan "TERMINATE", worker menghentikan proses dan mencatat jumlah pesan yang telah diproses sebelum keluar.

## **Bukti Hasil Program**
#### **Hasil `client.c`**
![Image](https://github.com/user-attachments/assets/cefd2e27-312e-4cc1-b1cb-eba964517d7e)
#### **`sistem.log`**
![Image](https://github.com/user-attachments/assets/dd64a9b3-0306-4ff0-9a61-f4e0ec87880f)
![Image](https://github.com/user-attachments/assets/1253feda-08f0-4efb-8055-d0a5e3b0a0c1)

#### **Hasil `loadbalancer.c`**
![Image](https://github.com/user-attachments/assets/ded5fe47-e589-4926-9708-d77ecf9062e8)
![Image](https://github.com/user-attachments/assets/2a434096-37e1-435f-8e9d-7aa0493df457)

#### **Hasil `worker.c`**
![Image](https://github.com/user-attachments/assets/14b1beae-b0d5-4045-a6ed-855b5f51e2d1)
![Image](https://github.com/user-attachments/assets/f31ff51e-ad42-4438-91ff-711d1af5f329)









