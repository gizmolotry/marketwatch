export function LoadingState({ label = "Loading market intelligence..." }) {
  return (
    <div className="state-block">
      <span className="pulse-dot" />
      {label}
    </div>
  );
}

export function ErrorState({ message }) {
  return <div className="state-block error-state">{message}</div>;
}
