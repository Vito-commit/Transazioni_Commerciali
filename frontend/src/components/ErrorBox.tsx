export function ErrorBox({ error }: { error: string }) {
    return (
        <div style={{ padding: 12, border: '1px solid #f99', background: '#fff5f5', borderRadius: 8 }}>
            <b>Errore:</b> {error}
        </div>
    );
}
