import React from "react";

export function View({ title }: { title: string }) {
  return <div className="view">{format(title)}</div>;
}

const format = (text: string) => text.toUpperCase();
