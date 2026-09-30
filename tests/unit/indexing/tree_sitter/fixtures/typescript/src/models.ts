import { Base, type Shape } from "./base";
import Logger from "../log/logger";

/** A tagged value. */
export interface Tagged extends Shape {
  tag: string;
  describe(): string;
}

export type Id = string | number;

export enum Color {
  Red,
  Green,
}

export abstract class Entity extends Base implements Tagged {
  static count = 0;
  tag = "entity";
  private readonly log = new Logger();
  handler = (event: string): void => {
    this.record(event);
  };

  constructor(public id: Id) {
    super();
  }

  get label(): string {
    return this.tag + String(this.id);
  }

  set label(value: string) {
    this.tag = value;
  }

  abstract describe(): string;

  record(event: string): void {
    this.log.write(event);
    helper(event);
    this.record(event);
  }
}

export namespace Geometry.Shapes {
  export function area(w: number, h: number): number {
    return w * h;
  }
}

function helper(text: string): string {
  return text.trim();
}

export const DEFAULT_TAG = "none";
export const shout = async (text: string) => helper(text).toUpperCase();
let counter = 0;
