import { useEffect, useMemo, useState } from "react";

export function PerfPill({ ms }: { ms: number | null }) {
    const [shown, setShown] = useState<number | null>(null);

    // stato "slow" solo sopra 5000ms
    const slow = useMemo(() => (ms !== null ? ms > 5000 : false), [ms]);

    useEffect(() => {
        if (ms === null) {
            setShown(null);
            return;
        }

        // count-up: da valore precedente -> nuovo in ~350ms
        const start = performance.now();
        const from = shown ?? 0;
        const to = ms;

        const duration = 350;
        let raf = 0;

        function tick(now: number) {
            const t = Math.min(1, (now - start) / duration);
            // easing
            const eased = 1 - Math.pow(1 - t, 3);
            const v = from + (to - from) * eased;
            setShown(v);

            if (t < 1) raf = requestAnimationFrame(tick);
        }

        raf = requestAnimationFrame(tick);
        return () => cancelAnimationFrame(raf);
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [ms]);

    return (
        <div className={`perfPill ${slow ? "perfPillSlow" : ""}`} title="Tempo ultima query">
            {shown === null ? "— ms" : `${Math.round(shown)} ms`}
        </div>
    );
}
