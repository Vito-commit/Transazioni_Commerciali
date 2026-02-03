export function TopBar(props: {
    right?: React.ReactNode;
}) {
    return (
        <div className="topbar">
            <div className="brand">
                <h1 className="brandTitle">DarkMarket Analytics</h1>
                <p className="brandSub">Query aggregate su transazioni commerciali (Spark + FastAPI + React)</p>
            </div>
            <div className="topRight">
                {props.right}
            </div>
        </div>
    );
}
