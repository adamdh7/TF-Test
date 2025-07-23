import { menu } from './menu';
import { getqr } from './technical/getqr';
import { getcode } from './technical/getcode';
import * as chat from './chatCommands';

export default {
  menu,
  getqr,
  getcode,
  ...chat,
};
