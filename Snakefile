# Snakefile (Project 2 orchestrator) - Windows safe

import os

OUTDIR = "out"
MARKDIR = os.path.join(OUTDIR, ".pipeline")

def touch(path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write("ok\n")

rule all:
    input:
        os.path.join(MARKDIR, "healthcheck.ok"),
        os.path.join(MARKDIR, "audio_uploaded.ok"),
        os.path.join(MARKDIR, "audio_classified.ok"),
        os.path.join(OUTDIR, "latest_report.csv")

# 1) Healthcheck: Mongo + MinIO
rule healthcheck:
    output:
        os.path.join(MARKDIR, "healthcheck.ok")
    run:
        os.makedirs(MARKDIR, exist_ok=True)
        shell("python src/healthcheck_services.py")
        touch(output[0])

# 2) Upload audio -> MinIO + Mongo metadata
rule upload_audio:
    input:
        os.path.join(MARKDIR, "healthcheck.ok")
    output:
        os.path.join(MARKDIR, "audio_uploaded.ok")
    run:
        os.makedirs(MARKDIR, exist_ok=True)
        shell("python src/upload_audio.py")
        touch(output[0])

# 3) Classify audio -> API + Mongo (+ optional MinIO logs)
rule classify_audio:
    input:
        os.path.join(MARKDIR, "audio_uploaded.ok")
    output:
        os.path.join(MARKDIR, "audio_classified.ok")
    run:
        os.makedirs(MARKDIR, exist_ok=True)
        shell("python src/classify_audio.py")
        touch(output[0])

# 4) Generate report -> out/latest_report.csv
rule generate_report:
    input:
        os.path.join(MARKDIR, "audio_classified.ok")
    output:
        os.path.join(OUTDIR, "latest_report.csv")
    params:
        min_score = os.getenv("POSITIVE_THRESHOLD", "0.3")
    run:
        os.makedirs(OUTDIR, exist_ok=True)

        shell(f"python src/generate_report.py --min-score {params.min_score}")

        # Copy newest timestamped report to latest_report.csv
        import glob, shutil
        reports = sorted(glob.glob(os.path.join(OUTDIR, "bird_report_*.csv")))
        if not reports:
            raise Exception("No report files found in out/ (expected bird_report_*.csv).")
        shutil.copyfile(reports[-1], output[0])
