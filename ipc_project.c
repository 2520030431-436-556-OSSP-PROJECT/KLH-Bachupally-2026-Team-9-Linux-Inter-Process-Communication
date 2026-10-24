/*
 * ipc_project.c
 * Project : Linux Inter-Process Communication Using Pipes and FIFOs
 * Course  : Operating Systems and Systems Programming (25CS2104E)
 *
 * ONE integrated application: a log-processing pipeline.
 * No input file is needed: the collector generates the sample log
 * messages (valid and deliberately invalid ones) inside the program.
 *
 *   parent (coordinator)
 *     |-- fork() --> COLLECTOR : generates sample log messages and
 *     |                          write()s them into an ANONYMOUS PIPE
 *     |-- fork() --> FILTER    : read() the pipe, validate + transform records,
 *     |                          write() them into a NAMED FIFO
 *     '-- fork() --> ANALYZER  : read() the FIFO, store records in a dynamic
 *                                buffer (page-fault measurement), run N POSIX
 *                                worker threads (mutex-protected statistics),
 *                                write() processed.log and report.txt
 *
 * CO mapping (see GUIDE.md for the full table)
 *   CO1 system calls : open/read/write/close/pipe/mkfifo/fork/waitpid/getrusage...
 *   CO2 fork()       : three child roles, waitpid() reaping, exit statuses
 *   CO3 pipe + FIFO  : collector->filter (pipe), filter->analyzer (FIFO)
 *   CO4 page faults  : getrusage() minor/major fault counters in the analyzer
 *   CO5 file I/O     : open/write/close/fstat for processed.log and report.txt
 *   CO6 pthreads     : pthread_create/join + pthread_mutex protecting stats
 *
 * Build : gcc -Wall -Wextra -Wpedantic -pthread ipc_project.c -o ipc_project
 * Run   : ./ipc_project -h
 */

#define _GNU_SOURCE

#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <signal.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <sys/prctl.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <sys/wait.h>

/* ------------------------------------------------------------------ */
/* Constants and types                                                 */
/* ------------------------------------------------------------------ */

#define LINE_CAP      1024      /* longest record we keep (longer = truncated) */
#define IO_BUF        65536     /* batch size for pipe/FIFO/file writes        */
#define MAX_THREADS   64
#define MAX_SERVICES  32
#define NUM_LEVELS    5
#define NUM_ROLES     3
#define VERBOSE_LIMIT 40        /* print every message only for the first N   */

static const char *LEVEL_NAMES[NUM_LEVELS] = {"DEBUG", "INFO", "WARN", "ERROR", "CRITICAL"};
static const char *ROLE_NAMES[NUM_ROLES]   = {"collector", "filter", "analyzer"};

/* Built-in sample messages.  has_ts = 1: the collector prepends a timestamp
 * ("DATE TIME ") so the line looks like a real log line.  has_ts = 0: the
 * text is sent exactly as written, so the filter has something to reject. */
typedef struct { int has_ts; const char *text; } sample_t;

static const sample_t SAMPLES[] = {
    {1, "INFO auth user=alice msg=login_ok"},
    {1, "info auth user=bob msg=login_ok"},                  /* lowercase: normalised */
    {1, "WARN db slow_query=1200ms table=orders"},
    {1, "ERROR payment txn=9981 msg=card_declined"},
    {1, "DEBUG cache key=user:42 hit=true"},
    {0, ""},                                                 /* INVALID: blank line   */
    {1, "INFO web GET /index.html 200"},
    {1, "warning web GET /missing 404"},                     /* WARNING -> WARN       */
    {1, "CRITICAL db msg=connection_pool_exhausted"},
    {0, "garbled#data###"},                                  /* INVALID: malformed    */
    {1, "INFO web POST /api/order 201"},
    {1, "ERROR auth user=mallory msg=bad_password"},
    {1, "INFO payment txn=9982 msg=captured"},
    {1, "BOGUS web unknown_level_dropped"},                  /* INVALID: bad level    */
    {1, "DEBUG web session=abc123 refreshed"},
    {0, "# comment line, not a log record"},                 /* INVALID: comment      */
    {1, "WARN auth user=carol msg=password_expiring"},
    {1, "INFO db checkpoint complete"},
    {0, "2026-10-24 INFO"},                                  /* INVALID: too few fields */
    {1, "ERROR web GET /api/report 500"},
    {1, "INFO cache evicted=128 keys"},
    {1, "INFO auth user=dave msg=logout"}
};
#define NUM_SAMPLES ((long)(sizeof SAMPLES / sizeof SAMPLES[0]))

typedef struct {
    const char *outdir;       /* directory for FIFO + output files       */
    int         threads;      /* number of worker threads                */
    long        repeat;       /* generate the sample set this many times */
    long        rounds;       /* CPU work (hash rounds) per record       */
    int         hold;         /* demo mode: collector waits N seconds    */
    char        fifo_path[512];
    char        out_path[512];
    char        report_path[512];
} config_t;

static volatile sig_atomic_t g_stop = 0;

static void on_signal(int sig)
{
    (void)sig;
    g_stop = 1;
}

/* ------------------------------------------------------------------ */
/* Small helpers: logging, full writes, line reader, output buffer     */
/* ------------------------------------------------------------------ */

/* write() may write fewer bytes than requested (partial write): loop. */
static int write_all(int fd, const void *buf, size_t len)
{
    const char *p = buf;
    while (len > 0) {
        ssize_t n = write(fd, p, len);
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        p   += n;
        len -= (size_t)n;
    }
    return 0;
}

/* One write() per log line so lines from different processes do not mix. */
static void logmsg(const char *role, const char *fmt, ...)
{
    char buf[1024];
    int n = snprintf(buf, sizeof buf, "[%-9s pid=%d ppid=%d] ", role, (int)getpid(), (int)getppid());
    va_list ap;
    va_start(ap, fmt);
    int m = vsnprintf(buf + n, sizeof buf - (size_t)n, fmt, ap);
    va_end(ap);
    if (m < 0) return;
    size_t len = (size_t)n + (size_t)m;
    if (len > sizeof buf - 2) len = sizeof buf - 2;
    buf[len++] = '\n';
    if (write_all(STDOUT_FILENO, buf, len) < 0) { /* nothing sensible to do */ }
}

/* Append formatted text to a fixed buffer (used to build the report). */
typedef struct { char buf[16384]; size_t len; } text_t;

static void tprintf(text_t *t, const char *fmt, ...)
{
    va_list ap;
    va_start(ap, fmt);
    int m = vsnprintf(t->buf + t->len, sizeof t->buf - t->len, fmt, ap);
    va_end(ap);
    if (m < 0) return;
    t->len += (size_t)m;
    if (t->len >= sizeof t->buf) t->len = sizeof t->buf - 1;
}

/* Buffered writer for a file descriptor (pipe, FIFO or file). */
typedef struct { int fd; size_t len; char buf[IO_BUF]; } outbuf_t;

static int ob_flush(outbuf_t *o)
{
    int rc = 0;
    if (o->len > 0) rc = write_all(o->fd, o->buf, o->len);
    o->len = 0;
    return rc;
}

static int ob_append(outbuf_t *o, const char *s, size_t n)
{
    if (n > sizeof o->buf) {                 /* too big to buffer: write directly */
        if (ob_flush(o) < 0) return -1;
        return write_all(o->fd, s, n);
    }
    if (o->len + n > sizeof o->buf && ob_flush(o) < 0) return -1;
    memcpy(o->buf + o->len, s, n);
    o->len += n;
    return 0;
}

/* Reads a descriptor and hands back one line at a time.  This is how we
 * deal with partial reads: read() can return half a line, or many lines. */
typedef struct { int fd; size_t start, end; int eof; char buf[4096]; } linereader_t;

static void copy_line(char *dst, size_t cap, const char *src, size_t len)
{
    if (len > 0 && src[len - 1] == '\r') len--;
    if (len > cap - 1) len = cap - 1;
    memcpy(dst, src, len);
    dst[len] = '\0';
}

/* returns 1 = got a line, 0 = EOF, -1 = read error */
static int read_line(linereader_t *lr, char *line, size_t cap)
{
    for (;;) {
        char  *base = lr->buf + lr->start;
        size_t have = lr->end - lr->start;
        char  *nl   = have ? memchr(base, '\n', have) : NULL;

        if (nl) {
            copy_line(line, cap, base, (size_t)(nl - base));
            lr->start += (size_t)(nl - base) + 1;
            return 1;
        }
        if (lr->eof) {
            if (have == 0) return 0;
            copy_line(line, cap, base, have);      /* last line without '\n' */
            lr->start = lr->end;
            return 1;
        }
        if (lr->start > 0) {                       /* make room: slide data down */
            memmove(lr->buf, base, have);
            lr->end   = have;
            lr->start = 0;
        }
        if (lr->end == sizeof lr->buf) {           /* absurdly long line: emit part */
            copy_line(line, cap, lr->buf, lr->end);
            lr->start = lr->end = 0;
            return 1;
        }
        ssize_t n = read(lr->fd, lr->buf + lr->end, sizeof lr->buf - lr->end);
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (n == 0) lr->eof = 1;                   /* EOF: all writers closed */
        else        lr->end += (size_t)n;
    }
}

static int level_index(const char *name)
{
    for (int i = 0; i < NUM_LEVELS; i++)
        if (strcmp(name, LEVEL_NAMES[i]) == 0) return i;
    return -1;
}

static double now_sec(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec / 1e9;
}

/* ------------------------------------------------------------------ */
/* CO4: page-fault counters                                            */
/* ------------------------------------------------------------------ */

typedef struct { long minflt, majflt; } faults_t;

/* RUSAGE_SELF = this process (all of its threads). It does NOT include
 * children.  Minor fault = page was not mapped yet but no disk I/O was
 * needed (e.g. first touch of freshly malloc'd memory).  Major fault =
 * disk I/O was needed (swap or file read). */
static faults_t faults_now(void)
{
    struct rusage ru;
    faults_t f = {0, 0};
    if (getrusage(RUSAGE_SELF, &ru) == 0) {
        f.minflt = ru.ru_minflt;
        f.majflt = ru.ru_majflt;
    }
    return f;
}

/* ------------------------------------------------------------------ */
/* ROLE 1: COLLECTOR  (generated messages  -->  anonymous pipe)        */
/* ------------------------------------------------------------------ */

static int run_collector(const config_t *cfg, int pipe_wr)
{
    prctl(PR_SET_NAME, "ipc_collector");

    logmsg("collector", "generating built-in sample messages (%ld per round, %ld round(s)); pipe write end fd=%d",
           NUM_SAMPLES, cfg->repeat, pipe_wr);

    if (cfg->hold > 0) {
        logmsg("collector", "demo mode: holding %d s before sending (inspect me now!)", cfg->hold);
        sleep((unsigned)cfg->hold);
    }

    outbuf_t *ob = malloc(sizeof *ob);
    if (!ob) { close(pipe_wr); return 2; }
    ob->fd = pipe_wr;
    ob->len = 0;

    unsigned long sent = 0;
    int rc = 0;
    for (long rep = 0; rep < cfg->repeat && rc == 0; rep++) {
        for (long i = 0; i < NUM_SAMPLES; i++) {
            char msg[LINE_CAP];
            int n;
            if (SAMPLES[i].has_ts) {
                /* Fake but realistic timestamp that advances with each message. */
                unsigned long sec = sent % 86400UL;
                n = snprintf(msg, sizeof msg, "2026-10-24 %02lu:%02lu:%02lu %s\n",
                             sec / 3600, (sec / 60) % 60, sec % 60, SAMPLES[i].text);
            } else {
                n = snprintf(msg, sizeof msg, "%s\n", SAMPLES[i].text);
            }
            if (n < 0) continue;
            if ((size_t)n >= sizeof msg) n = (int)sizeof msg - 1;

            if (sent < VERBOSE_LIMIT)
                logmsg("collector", "generated #%lu -> pipe: \"%.*s\"", sent + 1, n - 1, msg);

            if (ob_append(ob, msg, (size_t)n) < 0) {
                logmsg("collector", "ERROR: write to pipe: %s", strerror(errno));
                rc = 2;
                break;
            }
            sent++;
            if (sent % 100000 == 0)
                logmsg("collector", "progress: %lu messages sent into pipe", sent);
        }
    }
    if (rc == 0 && ob_flush(ob) < 0) rc = 2;

    close(pipe_wr);                     /* EOF for the filter */
    logmsg("collector", "done: %lu messages written to anonymous pipe", sent);
    free(ob);
    return rc;
}

/* ------------------------------------------------------------------ */
/* ROLE 2: FILTER  (anonymous pipe  -->  transform  -->  named FIFO)   */
/* ------------------------------------------------------------------ */

static int run_filter(const config_t *cfg, int pipe_rd)
{
    prctl(PR_SET_NAME, "ipc_filter");
    logmsg("filter", "pipe read end fd=%d; opening FIFO '%s' for writing (blocks until analyzer opens it)",
           pipe_rd, cfg->fifo_path);

    int ffd = open(cfg->fifo_path, O_WRONLY);
    if (ffd < 0) {
        logmsg("filter", "ERROR: open FIFO: %s", strerror(errno));
        close(pipe_rd);
        return 2;
    }
    logmsg("filter", "FIFO opened, fd=%d", ffd);

    outbuf_t *ob = malloc(sizeof *ob);
    linereader_t *lr = calloc(1, sizeof *lr);
    if (!ob || !lr) { free(ob); free(lr); close(ffd); close(pipe_rd); return 2; }
    ob->fd = ffd;
    ob->len = 0;
    lr->fd = pipe_rd;

    char line[LINE_CAP], out[LINE_CAP + 128];
    unsigned long seen = 0, forwarded = 0, dropped = 0;
    int rc = 0, r;

    while ((r = read_line(lr, line, sizeof line)) == 1) {
        seen++;
        int verbose = (seen <= VERBOSE_LIMIT);
        char date[16], tm[16], lvl[16], svc[48];
        int off = 0;

        /* Reject blank lines and comments. */
        const char *s = line;
        while (*s == ' ' || *s == '\t') s++;
        if (*s == '\0' || *s == '#') {
            dropped++;
            if (verbose) logmsg("filter", "REJECT #%lu (%s): \"%s\"", seen,
                                *s == '#' ? "comment" : "blank line", line);
            continue;
        }

        /* Expected format:  DATE TIME LEVEL SERVICE message... */
        if (sscanf(line, "%15s %15s %15s %47s %n", date, tm, lvl, svc, &off) < 4) {
            dropped++;
            if (verbose) logmsg("filter", "REJECT #%lu (malformed, too few fields): \"%s\"", seen, line);
            continue;
        }
        for (char *c = lvl; *c; c++) *c = (char)toupper((unsigned char)*c);
        if (strcmp(lvl, "WARNING") == 0) strcpy(lvl, "WARN");
        if (level_index(lvl) < 0) {
            dropped++;
            if (verbose) logmsg("filter", "REJECT #%lu (unknown level '%s'): \"%s\"", seen, lvl, line);
            continue;
        }

        forwarded++;
        int n = snprintf(out, sizeof out, "%lu|%s|%s|%s %s|%s\n",
                         forwarded, lvl, svc, date, tm, line + off);
        if (n < 0) continue;
        if ((size_t)n >= sizeof out) { n = (int)sizeof out - 1; out[n - 1] = '\n'; }
        if (verbose) logmsg("filter", "ACCEPT #%lu -> FIFO: \"%.*s\"", seen, n - 1, out);
        if (ob_append(ob, out, (size_t)n) < 0) {
            logmsg("filter", "ERROR: write to FIFO: %s", strerror(errno));
            rc = 2;
            break;
        }
        if (forwarded % 100000 == 0)
            logmsg("filter", "progress: %lu records forwarded into FIFO", forwarded);
    }
    if (r < 0) { logmsg("filter", "ERROR: read pipe: %s", strerror(errno)); rc = 2; }

    /* Trailer record so the analyzer learns the filter's statistics. */
    if (rc == 0) {
        int n = snprintf(out, sizeof out, "#STATS read=%lu forwarded=%lu dropped=%lu\n",
                         seen, forwarded, dropped);
        if (ob_append(ob, out, (size_t)n) < 0 || ob_flush(ob) < 0) rc = 2;
    }

    close(pipe_rd);
    close(ffd);                         /* EOF for the analyzer */
    logmsg("filter", "done: read=%lu forwarded=%lu dropped=%lu", seen, forwarded, dropped);
    free(ob);
    free(lr);
    return rc;
}

/* ------------------------------------------------------------------ */
/* ROLE 3: ANALYZER  (FIFO --> memory --> worker threads --> files)    */
/* ------------------------------------------------------------------ */

/* Records live in one growing arena; off[i] is where record i starts. */
typedef struct {
    char   *arena;
    size_t  used, cap;
    size_t *off;
    size_t  count, off_cap;
} store_t;

static int store_add(store_t *s, const char *rec)
{
    size_t need = strlen(rec) + 1;
    if (s->used + need > s->cap) {
        size_t ncap = s->cap ? s->cap * 2 : 4096;
        while (ncap < s->used + need) ncap *= 2;
        char *p = realloc(s->arena, ncap);
        if (!p) return -1;
        s->arena = p;
        s->cap = ncap;
    }
    if (s->count == s->off_cap) {
        size_t ncap = s->off_cap ? s->off_cap * 2 : 256;
        size_t *p = realloc(s->off, ncap * sizeof *p);
        if (!p) return -1;
        s->off = p;
        s->off_cap = ncap;
    }
    memcpy(s->arena + s->used, rec, need);   /* first touch of new pages -> minor faults */
    s->off[s->count++] = s->used;
    s->used += need;
    return 0;
}

/* State shared by all worker threads.  Everything marked "protected"
 * is only touched while holding 'lock'. */
typedef struct {
    pthread_mutex_t lock;
    const store_t  *store;               /* read-only while workers run */
    uint32_t       *chk;                 /* chk[i] written only by the worker that claimed i */
    long            rounds;
    size_t          next;                /* protected: next record index to claim */
    unsigned long   processed;           /* protected */
    unsigned long   level_count[NUM_LEVELS]; /* protected */
    uint32_t        checksum_xor;        /* protected */
    char            svc_name[MAX_SERVICES][32];  /* protected */
    unsigned long   svc_count[MAX_SERVICES];     /* protected */
    int             svc_used;            /* protected */
    unsigned long   per_worker[MAX_THREADS];     /* protected */
} shared_t;

typedef struct { shared_t *sh; int id; } worker_arg_t;

/* CPU work for one record: FNV-1a hash repeated 'rounds' times. */
static uint32_t record_checksum(const char *rec, long rounds)
{
    uint32_t h = 2166136261u;
    for (long r = 0; r < rounds; r++)
        for (const char *p = rec; *p; p++) {
            h ^= (unsigned char)*p;
            h *= 16777619u;
        }
    return h;
}

static void *worker_main(void *arg)
{
    worker_arg_t *wa = arg;
    shared_t *sh = wa->sh;
    char name[16];
    snprintf(name, sizeof name, "ipc_worker_%d", wa->id);
    pthread_setname_np(pthread_self(), name);

    logmsg(name, "started, kernel thread id (TID)=%ld", (long)syscall(SYS_gettid));

    unsigned long mine = 0;
    for (;;) {
        pthread_mutex_lock(&sh->lock);
        size_t idx = sh->next;
        if (idx < sh->store->count) sh->next++;
        pthread_mutex_unlock(&sh->lock);
        if (idx >= sh->store->count) break;

        const char *rec = sh->store->arena + sh->store->off[idx];

        /* Parse "seq|LEVEL|service|timestamp|message" (no lock needed: read-only). */
        char lvl[16] = "", svc[32] = "";
        const char *a = strchr(rec, '|');
        if (a) {
            const char *b = strchr(a + 1, '|');
            if (b) {
                size_t n = (size_t)(b - (a + 1));
                if (n >= sizeof lvl) n = sizeof lvl - 1;
                memcpy(lvl, a + 1, n); lvl[n] = '\0';
                const char *c = strchr(b + 1, '|');
                if (c) {
                    n = (size_t)(c - (b + 1));
                    if (n >= sizeof svc) n = sizeof svc - 1;
                    memcpy(svc, b + 1, n); svc[n] = '\0';
                }
            }
        }
        int li = level_index(lvl);

        uint32_t h = record_checksum(rec, sh->rounds);   /* the CPU-heavy part */
        sh->chk[idx] = h;

        /* Critical section: update shared statistics. */
        pthread_mutex_lock(&sh->lock);
        sh->processed++;
        if (li >= 0) sh->level_count[li]++;
        sh->checksum_xor ^= h;
        sh->per_worker[wa->id]++;
        int s;
        for (s = 0; s < sh->svc_used; s++)
            if (strcmp(sh->svc_name[s], svc) == 0) break;
        if (s == sh->svc_used && s < MAX_SERVICES) {
            snprintf(sh->svc_name[s], sizeof sh->svc_name[s], "%s", svc);
            sh->svc_used++;
        }
        if (s < MAX_SERVICES) sh->svc_count[s]++;
        pthread_mutex_unlock(&sh->lock);
        mine++;
    }
    logmsg(name, "finished, processed %lu records", mine);
    return NULL;
}

static int write_file(const char *path, const char *data, size_t len, struct stat *st_out)
{
    int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) return -1;
    int rc = write_all(fd, data, len);
    if (rc == 0 && st_out && fstat(fd, st_out) < 0) rc = -1;
    if (close(fd) < 0) rc = -1;
    return rc;
}

static int run_analyzer(const config_t *cfg)
{
    prctl(PR_SET_NAME, "ipc_analyzer");
    logmsg("analyzer", "opening FIFO '%s' for reading (blocks until filter opens it)", cfg->fifo_path);

    int ffd = open(cfg->fifo_path, O_RDONLY);
    if (ffd < 0) {
        logmsg("analyzer", "ERROR: open FIFO: %s", strerror(errno));
        return 3;
    }
    logmsg("analyzer", "FIFO opened, fd=%d", ffd);

    double t0 = now_sec();
    faults_t pf0 = faults_now();                 /* before loading records */

    /* ---- Stage 1: read records from the FIFO into dynamic memory ---- */
    store_t store = {0};
    linereader_t *lr = calloc(1, sizeof *lr);
    if (!lr) { close(ffd); return 3; }
    lr->fd = ffd;

    char line[LINE_CAP + 128];
    unsigned long f_read = 0, f_fwd = 0, f_drop = 0;
    int got_stats = 0, rc = 0, r;
    while ((r = read_line(lr, line, sizeof line)) == 1) {
        if (line[0] == '#') {
            if (sscanf(line, "#STATS read=%lu forwarded=%lu dropped=%lu",
                       &f_read, &f_fwd, &f_drop) == 3) got_stats = 1;
            continue;
        }
        if (store_add(&store, line) < 0) {
            logmsg("analyzer", "ERROR: out of memory");
            rc = 3;
            break;
        }
    }
    if (r < 0) { logmsg("analyzer", "ERROR: read FIFO: %s", strerror(errno)); rc = 3; }
    close(ffd);
    free(lr);
    faults_t pf1 = faults_now();                 /* after loading records */
    logmsg("analyzer", "loaded %zu records (%zu bytes of arena) from FIFO", store.count, store.used);
    if (rc != 0) { free(store.arena); free(store.off); return rc; }

    /* ---- Stage 2: worker threads process the records ---- */
    shared_t *sh = calloc(1, sizeof *sh);
    uint32_t *chk = calloc(store.count + 1, sizeof *chk);
    if (!sh || !chk) { free(sh); free(chk); free(store.arena); free(store.off); return 3; }
    pthread_mutex_init(&sh->lock, NULL);
    sh->store  = &store;
    sh->chk    = chk;
    sh->rounds = cfg->rounds;

    pthread_t    tids[MAX_THREADS];
    worker_arg_t args[MAX_THREADS];
    int created = 0;
    for (int i = 0; i < cfg->threads; i++) {
        args[i].sh = sh;
        args[i].id = i;
        int e = pthread_create(&tids[i], NULL, worker_main, &args[i]);
        if (e != 0) {
            logmsg("analyzer", "ERROR: pthread_create(%d): %s", i, strerror(e));
            break;
        }
        created++;
    }
    if (created == 0) {
        pthread_mutex_destroy(&sh->lock);
        free(sh); free(chk); free(store.arena); free(store.off);
        return 3;
    }
    for (int i = 0; i < created; i++) pthread_join(tids[i], NULL);
    pthread_mutex_destroy(&sh->lock);
    faults_t pf2 = faults_now();                 /* after worker processing */
    double t1 = now_sec();

    /* ---- Stage 3: write processed.log (file I/O) ---- */
    struct stat out_st;
    memset(&out_st, 0, sizeof out_st);
    {
        int ofd = open(cfg->out_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (ofd < 0) {
            logmsg("analyzer", "ERROR: cannot create '%s': %s", cfg->out_path, strerror(errno));
            rc = 3;
        } else {
            outbuf_t *ob = malloc(sizeof *ob);
            if (!ob) rc = 3;
            else {
                ob->fd = ofd;
                ob->len = 0;
                char outline[LINE_CAP + 64];
                for (size_t i = 0; i < store.count && rc == 0; i++) {
                    int n = snprintf(outline, sizeof outline, "%s|chk=%08x\n",
                                     store.arena + store.off[i], (unsigned)chk[i]);
                    if (n < 0) continue;
                    if ((size_t)n >= sizeof outline) n = (int)sizeof outline - 1;
                    if (ob_append(ob, outline, (size_t)n) < 0) rc = 3;
                }
                if (rc == 0 && ob_flush(ob) < 0) rc = 3;
                free(ob);
            }
            if (rc == 0 && fstat(ofd, &out_st) < 0) rc = 3;
            if (close(ofd) < 0) rc = 3;
            if (rc != 0) logmsg("analyzer", "ERROR: writing '%s' failed", cfg->out_path);
        }
    }

    /* ---- Stage 4: final report ---- */
    struct rusage ru;
    getrusage(RUSAGE_SELF, &ru);
    text_t *t = calloc(1, sizeof *t);
    if (t) {
        tprintf(t, "================ IPC LOG PROCESSING REPORT ================\n");
        tprintf(t, "Message source        : built-in generator (%ld messages x %ld round(s))\n",
                NUM_SAMPLES, cfg->repeat);
        tprintf(t, "Filter (via FIFO)     : read=%lu forwarded=%lu dropped=%lu%s\n",
                f_read, f_fwd, f_drop, got_stats ? "" : "  (no STATS trailer received!)");
        tprintf(t, "Analyzer received     : %zu records\n", store.count);
        tprintf(t, "Worker threads        : %d requested, %d created, rounds/record=%ld\n",
                cfg->threads, created, cfg->rounds);
        tprintf(t, "Records processed     : %lu\n", sh->processed);
        tprintf(t, "Level counts          :");
        for (int i = 0; i < NUM_LEVELS; i++) tprintf(t, " %s=%lu", LEVEL_NAMES[i], sh->level_count[i]);
        tprintf(t, "\nService counts        :");
        for (int i = 0; i < sh->svc_used; i++) tprintf(t, " %s=%lu", sh->svc_name[i], sh->svc_count[i]);
        tprintf(t, "\nPer-worker records    :");
        for (int i = 0; i < created; i++) tprintf(t, " w%d=%lu", i, sh->per_worker[i]);
        tprintf(t, "\nChecksum (XOR of all) : %08x   (same for any thread count)\n", (unsigned)sh->checksum_xor);
        tprintf(t, "Processing time      : %.3f s (FIFO read + workers)\n", t1 - t0);
        tprintf(t, "\n--- Page faults (analyzer process pid=%d, getrusage RUSAGE_SELF) ---\n", (int)getpid());
        tprintf(t, "Before loading records : minor=%ld major=%ld\n", pf0.minflt, pf0.majflt);
        tprintf(t, "After  loading records : minor=%ld major=%ld  (delta minor=%ld major=%ld)\n",
                pf1.minflt, pf1.majflt, pf1.minflt - pf0.minflt, pf1.majflt - pf0.majflt);
        tprintf(t, "After  worker threads  : minor=%ld major=%ld  (delta minor=%ld major=%ld)\n",
                pf2.minflt, pf2.majflt, pf2.minflt - pf1.minflt, pf2.majflt - pf1.majflt);
        tprintf(t, "Peak RSS (ru_maxrss)   : %ld KB\n", ru.ru_maxrss);
        tprintf(t, "\n--- Output file ---\n");
        tprintf(t, "%s : inode=%lu size=%ld bytes\n", cfg->out_path,
                (unsigned long)out_st.st_ino, (long)out_st.st_size);
        tprintf(t, "===========================================================\n");

        if (write_file(cfg->report_path, t->buf, t->len, NULL) < 0) {
            logmsg("analyzer", "ERROR: cannot write '%s': %s", cfg->report_path, strerror(errno));
            rc = 3;
        }
        if (write_all(STDOUT_FILENO, t->buf, t->len) < 0) { /* ignore */ }
        free(t);
    } else rc = 3;

    free(sh);
    free(chk);
    free(store.arena);
    free(store.off);
    logmsg("analyzer", "done");
    return rc;
}

/* ------------------------------------------------------------------ */
/* PARENT: setup, fork(), waitpid(), cleanup                           */
/* ------------------------------------------------------------------ */

static void usage(const char *prog)
{
    printf("Usage: %s [options]\n"
           "  (no input file needed: sample log messages are generated inside the program)\n"
           "  -o DIR    output directory          (default output)\n"
           "  -t N      worker threads, 1..%d      (default 4)\n"
           "  -r N      generate the sample set N times (default 1) -> bigger workload\n"
           "  -k N      hash rounds per record    (default 200) -> more CPU per record\n"
           "  -H SEC    demo hold: collector waits SEC seconds before sending data,\n"
           "            so all processes and descriptors can be inspected (default 0)\n"
           "  -h        this help\n", prog, MAX_THREADS);
}

static int parse_long(const char *s, long min, long max, long *out)
{
    char *end;
    errno = 0;
    long v = strtol(s, &end, 10);
    if (errno || *end != '\0' || end == s || v < min || v > max) return -1;
    *out = v;
    return 0;
}

static int parse_args(int argc, char **argv, config_t *cfg)
{
    int opt;
    long v;
    while ((opt = getopt(argc, argv, "o:t:r:k:H:h")) != -1) {
        switch (opt) {
        case 'o': cfg->outdir = optarg; break;
        case 't': if (parse_long(optarg, 1, MAX_THREADS, &v)) { fprintf(stderr, "bad -t\n"); return -1; } cfg->threads = (int)v; break;
        case 'r': if (parse_long(optarg, 1, 100000000L, &v)) { fprintf(stderr, "bad -r\n"); return -1; } cfg->repeat = v; break;
        case 'k': if (parse_long(optarg, 1, 100000000L, &v)) { fprintf(stderr, "bad -k\n"); return -1; } cfg->rounds = v; break;
        case 'H': if (parse_long(optarg, 0, 3600, &v)) { fprintf(stderr, "bad -H\n"); return -1; } cfg->hold = (int)v; break;
        case 'h': usage(argv[0]); return 1;
        default:  usage(argv[0]); return -1;
        }
    }
    return 0;
}

static void kill_remaining(const pid_t *pids, const int *reaped)
{
    for (int i = 0; i < NUM_ROLES; i++)
        if (pids[i] > 0 && !reaped[i]) kill(pids[i], SIGTERM);
}

int main(int argc, char **argv)
{
    config_t cfg = { "output", 4, 1, 200, 0, "", "", "" };
    int pr = parse_args(argc, argv, &cfg);
    if (pr != 0) return pr > 0 ? 0 : 1;

    snprintf(cfg.fifo_path,   sizeof cfg.fifo_path,   "%s/ipc_demo.fifo", cfg.outdir);
    snprintf(cfg.out_path,    sizeof cfg.out_path,    "%s/processed.log", cfg.outdir);
    snprintf(cfg.report_path, sizeof cfg.report_path, "%s/report.txt",    cfg.outdir);

    /* Create the output directory if it does not exist yet. */
    if (mkdir(cfg.outdir, 0755) < 0 && errno != EEXIST) {
        fprintf(stderr, "error: cannot create '%s': %s\n", cfg.outdir, strerror(errno));
        return 1;
    }

    /* CO3: create the named FIFO (remove a stale one left by a crashed run). */
    if (mkfifo(cfg.fifo_path, 0600) < 0) {
        if (errno == EEXIST) {
            unlink(cfg.fifo_path);
            if (mkfifo(cfg.fifo_path, 0600) < 0) {
                fprintf(stderr, "error: mkfifo '%s': %s\n", cfg.fifo_path, strerror(errno));
                return 1;
            }
        } else {
            fprintf(stderr, "error: mkfifo '%s': %s\n", cfg.fifo_path, strerror(errno));
            return 1;
        }
    }

    /* CO3: create the anonymous pipe BEFORE fork() so children inherit it. */
    int pfd[2];
    if (pipe(pfd) < 0) {
        fprintf(stderr, "error: pipe: %s\n", strerror(errno));
        unlink(cfg.fifo_path);
        return 1;
    }

    struct sigaction sa;
    memset(&sa, 0, sizeof sa);
    sa.sa_handler = on_signal;               /* no SA_RESTART: waitpid() gets EINTR */
    sigemptyset(&sa.sa_mask);
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);
    signal(SIGPIPE, SIG_IGN);                /* write() will return EPIPE instead of killing us */

    logmsg("parent", "started. threads=%d repeat=%ld rounds=%ld hold=%d (messages generated internally)",
           cfg.threads, cfg.repeat, cfg.rounds, cfg.hold);
    logmsg("parent", "pipe fds: read=%d write=%d | FIFO: %s", pfd[0], pfd[1], cfg.fifo_path);

    /* CO2: create the three child processes. */
    pid_t pids[NUM_ROLES] = {-1, -1, -1};
    int reaped[NUM_ROLES] = {0, 0, 0};
    int failed = 0, spawned = 0;

    for (int role = 0; role < NUM_ROLES; role++) {
        fflush(NULL);                         /* never fork with unflushed stdio data */
        pid_t pid = fork();
        if (pid < 0) {
            fprintf(stderr, "error: fork (%s): %s\n", ROLE_NAMES[role], strerror(errno));
            failed = 1;
            break;
        }
        if (pid == 0) {                       /* ---------- CHILD ---------- */
            signal(SIGINT,  SIG_DFL);
            signal(SIGTERM, SIG_DFL);
            int rc;
            if (role == 0) {                  /* collector: needs only pipe write end */
                close(pfd[0]);
                rc = run_collector(&cfg, pfd[1]);
            } else if (role == 1) {           /* filter: needs only pipe read end */
                close(pfd[1]);
                rc = run_filter(&cfg, pfd[0]);
            } else {                          /* analyzer: needs no pipe end at all */
                close(pfd[0]);
                close(pfd[1]);
                rc = run_analyzer(&cfg);
            }
            _exit(rc);
        }
        pids[role] = pid;                     /* ---------- PARENT ---------- */
        spawned++;
        logmsg("parent", "forked %-9s child pid=%d", ROLE_NAMES[role], (int)pid);
    }

    /* Parent must close its copies, otherwise the filter would never see EOF. */
    close(pfd[0]);
    close(pfd[1]);

    if (failed) kill_remaining(pids, reaped);

    logmsg("parent", "inspect from another terminal: ps -ef --forest | grep ipc_project");

    /* CO2: wait for every child (no zombies) and report exit statuses. */
    int remaining = spawned, killed_all = failed;
    while (remaining > 0) {
        if (g_stop && !killed_all) {
            logmsg("parent", "interrupted: asking children to stop");
            kill_remaining(pids, reaped);
            killed_all = 1;
            failed = 1;
        }
        int status;
        pid_t w = waitpid(-1, &status, 0);
        if (w < 0) {
            if (errno == EINTR) continue;
            if (errno == ECHILD) break;
            fprintf(stderr, "error: waitpid: %s\n", strerror(errno));
            break;
        }
        for (int i = 0; i < NUM_ROLES; i++) {
            if (pids[i] != w) continue;
            reaped[i] = 1;
            remaining--;
            if (WIFEXITED(status)) {
                logmsg("parent", "reaped %-9s pid=%d exit status=%d", ROLE_NAMES[i], (int)w, WEXITSTATUS(status));
                if (WEXITSTATUS(status) != 0) failed = 1;
            } else if (WIFSIGNALED(status)) {
                logmsg("parent", "reaped %-9s pid=%d killed by signal %d", ROLE_NAMES[i], (int)w, WTERMSIG(status));
                failed = 1;
            }
            if (failed && !killed_all) {      /* one role died: unblock the others */
                kill_remaining(pids, reaped);
                killed_all = 1;
            }
        }
    }

    struct rusage cru;
    if (getrusage(RUSAGE_CHILDREN, &cru) == 0)
        logmsg("parent", "page faults of all reaped children (RUSAGE_CHILDREN): minor=%ld major=%ld",
               cru.ru_minflt, cru.ru_majflt);

    if (unlink(cfg.fifo_path) < 0 && errno != ENOENT)
        fprintf(stderr, "warning: could not remove FIFO: %s\n", strerror(errno));
    else
        logmsg("parent", "FIFO removed. %s", failed ? "Finished WITH ERRORS." : "All stages finished successfully.");

    return failed ? 1 : 0;
}
