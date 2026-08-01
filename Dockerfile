FROM docker.io/denoland/deno:2.6.9

WORKDIR /app
RUN chown deno:deno /app

USER deno

VOLUME /app/dat
VOLUME /app/data

COPY --chown=deno:deno . .

# Deno 2.x: install the exact dependencies recorded in deno.lock. Runtime
# permissions remain attached to `deno task run` rather than the install step.
RUN deno install --frozen

CMD [ "deno", "task", "run" ]
