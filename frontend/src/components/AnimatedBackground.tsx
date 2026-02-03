import { useEffect, useRef } from "react";

type P = { x: number; y: number; vx: number; vy: number; r: number; a: number };

export function AnimatedBackground() {
    const ref = useRef<HTMLCanvasElement | null>(null);

    useEffect(() => {
        const canvas = ref.current!;
        const ctx = canvas.getContext("2d")!;
        let raf = 0;

        const dpr = Math.max(1, Math.floor(window.devicePixelRatio || 1));
        const st = { w: 0, h: 0, t: 0, mx: 0.5, my: 0.5 };

        const N = 42;
        const ps: P[] = Array.from({ length: N }, () => ({
            x: Math.random(),
            y: Math.random(),
            vx: (Math.random() - 0.5) * 0.00035,
            vy: (Math.random() - 0.5) * 0.00035,
            r: 0.8 + Math.random() * 1.8,
            a: 0.10 + Math.random() * 0.18,
        }));

        function resize() {
            st.w = window.innerWidth;
            st.h = window.innerHeight;
            canvas.width = Math.floor(st.w * dpr);
            canvas.height = Math.floor(st.h * dpr);
            canvas.style.width = st.w + "px";
            canvas.style.height = st.h + "px";
            ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
        }

        function onMove(e: MouseEvent) {
            st.mx = e.clientX / Math.max(1, st.w);
            st.my = e.clientY / Math.max(1, st.h);
        }

        function stepParticles() {
            for (const p of ps) {
                p.x += p.vx;
                p.y += p.vy;

                // soft mouse attraction (minimal)
                const dx = st.mx - p.x;
                const dy = st.my - p.y;
                p.vx += dx * 0.0000022;
                p.vy += dy * 0.0000022;

                // damping
                p.vx *= 0.985;
                p.vy *= 0.985;

                // wrap
                if (p.x < -0.1) p.x = 1.1;
                if (p.x > 1.1) p.x = -0.1;
                if (p.y < -0.1) p.y = 1.1;
                if (p.y > 1.1) p.y = -0.1;
            }
        }

        function draw() {
            st.t += 0.008;
            const w = st.w, h = st.h;

            // base
            ctx.clearRect(0, 0, w, h);
            ctx.fillStyle = "#0b1020";
            ctx.fillRect(0, 0, w, h);

            // glow blobs (mouse-reactive)
            const cx = w * (0.18 + st.mx * 0.64);
            const cy = h * (0.15 + st.my * 0.70);

            const blobs = [
                { x: cx, y: cy, r: 340 + 40 * Math.sin(st.t), a: 0.22 },
                { x: w * 0.86, y: h * 0.18, r: 360 + 30 * Math.cos(st.t * 1.2), a: 0.16 },
                { x: w * 0.72, y: h * 0.84, r: 420 + 25 * Math.sin(st.t * 0.9), a: 0.13 },
            ];

            for (const b of blobs) {
                const g = ctx.createRadialGradient(b.x, b.y, 0, b.x, b.y, b.r);
                g.addColorStop(0, `rgba(95,130,255,${b.a})`);
                g.addColorStop(0.55, `rgba(140,90,255,${b.a * 0.60})`);
                g.addColorStop(1, "rgba(0,0,0,0)");
                ctx.fillStyle = g;
                ctx.beginPath();
                ctx.arc(b.x, b.y, b.r, 0, Math.PI * 2);
                ctx.fill();
            }

            // particles
            stepParticles();
            ctx.globalAlpha = 1;

            // connections
            ctx.lineWidth = 1;
            for (let i = 0; i < ps.length; i++) {
                for (let j = i + 1; j < ps.length; j++) {
                    const ax = ps[i].x * w, ay = ps[i].y * h;
                    const bx = ps[j].x * w, by = ps[j].y * h;
                    const dx = ax - bx, dy = ay - by;
                    const dist = Math.sqrt(dx * dx + dy * dy);
                    const max = 160;
                    if (dist < max) {
                        const alpha = (1 - dist / max) * 0.08;
                        ctx.strokeStyle = `rgba(233,238,252,${alpha})`;
                        ctx.beginPath();
                        ctx.moveTo(ax, ay);
                        ctx.lineTo(bx, by);
                        ctx.stroke();
                    }
                }
            }

            // dots
            for (const p of ps) {
                const x = p.x * w, y = p.y * h;
                ctx.fillStyle = `rgba(233,238,252,${p.a})`;
                ctx.beginPath();
                ctx.arc(x, y, p.r, 0, Math.PI * 2);
                ctx.fill();
            }

            // very subtle noise overlay
            ctx.globalAlpha = 0.08;
            ctx.fillStyle = "#e9eefc";
            for (let k = 0; k < 80; k++) {
                const x = (Math.random() * w) | 0;
                const y = (Math.random() * h) | 0;
                ctx.fillRect(x, y, 1, 1);
            }
            ctx.globalAlpha = 1;

            raf = requestAnimationFrame(draw);
        }

        resize();
        window.addEventListener("resize", resize);
        window.addEventListener("mousemove", onMove);
        raf = requestAnimationFrame(draw);

        return () => {
            cancelAnimationFrame(raf);
            window.removeEventListener("resize", resize);
            window.removeEventListener("mousemove", onMove);
        };
    }, []);

    return <canvas ref={ref} className="bgCanvas" />;
}
