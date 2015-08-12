/*
Malom, a Nine Men's Morris (and variants) player and solver program.
Copyright(C) 2007-2014  Gabor E. Gevay, Gabor Danner

See our webpage (and the paper linked from there):
http://compalg.inf.elte.hu/~ggevay/mills/index.php


This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

package malom;

public class Symmetries {

	public static long minSym48(long a) {
		// TODO: Could be sped up by lookuptables
		long mi = Long.MAX_VALUE;
		for(int i=0; i<16; i++) {
			mi=Long.min(mi, sym48(i, a));
		}
		return mi;
	}

	public static boolean isFiltered(long a) {
		return minSym48(a) < a;
	}




	static final int patsize = 8, patc = 1 << patsize;

	static final int[][] table1=new int[16][patc], table2=new int[16][patc], table3=new int[16][patc]; // ~64 KB

	static final Sym[] slow = new Sym[]{
			Symmetries::rot90, Symmetries::rot180, Symmetries::rot270,
			Symmetries::tt_fuggoleges, Symmetries::tt_vizszintes, Symmetries::tt_bslash, Symmetries::tt_slash,
			Symmetries::swap, Symmetries::swap_rot90, Symmetries::swap_rot180, Symmetries::swap_rot270,
			Symmetries::swap_tt_fuggoleges, Symmetries::swap_tt_vizszintes, Symmetries::swap_tt_bslash, Symmetries::swap_tt_slash,
			Symmetries::id};

	static {
		System.out.println("init_sym_lookuptables");

		for(int pat=0; pat<patc; pat++){
		/*for(int i=0; i<16; i++)
			table1[i][pat] = slow[i](pat << 0);
		for(int i=0; i<16; i++)
			table2[i][pat] = slow[i](pat << 6);
		for(int i=0; i<16; i++)
			table3[i][pat] = slow[i](pat << 12);
		for(int i=0; i<16; i++)
			table4[i][pat] = slow[i](pat << 18);*/

		/*for(int k = 0; k < patn; k++)
			for(int i = 0; i<16; i++)
				table[k][i][pat] = slow[i](pat << k*patsize);*/

			for(int i = 0; i<16; i++)
				table1[i][pat] = slow[i].transform(pat << 0);
			for(int i = 0; i<16; i++)
				table2[i][pat] = slow[i].transform(pat << 8);
			for(int i = 0; i<16; i++)
				table3[i][pat] = slow[i].transform(pat << 16);
		}
	}

	static final long maskPatsize = (1 << patsize) - 1;

	static long sym24(int op, long a){
		long b=0;

		/*b |= table1[op][(a>>0)&mask];
		b |= table2[op][(a>>6)&mask];
		b |= table3[op][(a>>12)&mask];
		b |= table4[op][(a>>18)&mask];*/

		/*for(int k = 0; k < patn; k++)
			b |= table[k][op][(a >> k*patsize)&mask];*/

		b |= table1[op][(int)((a >> 0)&maskPatsize)];
		b |= table2[op][(int)((a >> 8)&maskPatsize)];
		b |= table3[op][(int)((a >> 16)&maskPatsize)];

		return b;
	}

	static final long mask24 = (1<<24) - 1;

	static long sym48(int op, long a){
		return sym24(op, a&mask24) | (sym24(op, a>>24)<<24);
	}






	interface Sym {
		int transform(int a);
	}

	static int rot90(int a) {
		int b = 0;
		b |= (((1 << 0) & a) >> 0) << 2;
		b |= (((1 << 1) & a) >> 1) << 3;
		b |= (((1 << 2) & a) >> 2) << 4;
		b |= (((1 << 3) & a) >> 3) << 5;
		b |= (((1 << 4) & a) >> 4) << 6;
		b |= (((1 << 5) & a) >> 5) << 7;
		b |= (((1 << 6) & a) >> 6) << 0;
		b |= (((1 << 7) & a) >> 7) << 1;
		b |= (((1 << 8) & a) >> 8) << 10;
		b |= (((1 << 9) & a) >> 9) << 11;
		b |= (((1 << 10) & a) >> 10) << 12;
		b |= (((1 << 11) & a) >> 11) << 13;
		b |= (((1 << 12) & a) >> 12) << 14;
		b |= (((1 << 13) & a) >> 13) << 15;
		b |= (((1 << 14) & a) >> 14) << 8;
		b |= (((1 << 15) & a) >> 15) << 9;
		b |= (((1 << 16) & a) >> 16) << 18;
		b |= (((1 << 17) & a) >> 17) << 19;
		b |= (((1 << 18) & a) >> 18) << 20;
		b |= (((1 << 19) & a) >> 19) << 21;
		b |= (((1 << 20) & a) >> 20) << 22;
		b |= (((1 << 21) & a) >> 21) << 23;
		b |= (((1 << 22) & a) >> 22) << 16;
		b |= (((1 << 23) & a) >> 23) << 17;
		return b;
	}

	static int rot180(int a) {
		return rot90(rot90(a));
	}

	static int rot270(int a) {
		return rot180(rot90(a));
	}

	static int tt_fuggoleges(int a) {
		int b = 0;
		b |= (((1 << 0) & a) >> 0) << 4;
		b |= (((1 << 1) & a) >> 1) << 3;
		b |= (((1 << 2) & a) >> 2) << 2;
		b |= (((1 << 3) & a) >> 3) << 1;
		b |= (((1 << 4) & a) >> 4) << 0;
		b |= (((1 << 5) & a) >> 5) << 7;
		b |= (((1 << 6) & a) >> 6) << 6;
		b |= (((1 << 7) & a) >> 7) << 5;
		b |= (((1 << 8) & a) >> 8) << 12;
		b |= (((1 << 9) & a) >> 9) << 11;
		b |= (((1 << 10) & a) >> 10) << 10;
		b |= (((1 << 11) & a) >> 11) << 9;
		b |= (((1 << 12) & a) >> 12) << 8;
		b |= (((1 << 13) & a) >> 13) << 15;
		b |= (((1 << 14) & a) >> 14) << 14;
		b |= (((1 << 15) & a) >> 15) << 13;
		b |= (((1 << 16) & a) >> 16) << 20;
		b |= (((1 << 17) & a) >> 17) << 19;
		b |= (((1 << 18) & a) >> 18) << 18;
		b |= (((1 << 19) & a) >> 19) << 17;
		b |= (((1 << 20) & a) >> 20) << 16;
		b |= (((1 << 21) & a) >> 21) << 23;
		b |= (((1 << 22) & a) >> 22) << 22;
		b |= (((1 << 23) & a) >> 23) << 21;
		return b;
	}

	static int tt_vizszintes(int a) {
		int b = 0;
		b |= (((1 << 0) & a) >> 0) << 0;
		b |= (((1 << 1) & a) >> 1) << 7;
		b |= (((1 << 2) & a) >> 2) << 6;
		b |= (((1 << 3) & a) >> 3) << 5;
		b |= (((1 << 4) & a) >> 4) << 4;
		b |= (((1 << 5) & a) >> 5) << 3;
		b |= (((1 << 6) & a) >> 6) << 2;
		b |= (((1 << 7) & a) >> 7) << 1;
		b |= (((1 << 8) & a) >> 8) << 8;
		b |= (((1 << 9) & a) >> 9) << 15;
		b |= (((1 << 10) & a) >> 10) << 14;
		b |= (((1 << 11) & a) >> 11) << 13;
		b |= (((1 << 12) & a) >> 12) << 12;
		b |= (((1 << 13) & a) >> 13) << 11;
		b |= (((1 << 14) & a) >> 14) << 10;
		b |= (((1 << 15) & a) >> 15) << 9;
		b |= (((1 << 16) & a) >> 16) << 16;
		b |= (((1 << 17) & a) >> 17) << 23;
		b |= (((1 << 18) & a) >> 18) << 22;
		b |= (((1 << 19) & a) >> 19) << 21;
		b |= (((1 << 20) & a) >> 20) << 20;
		b |= (((1 << 21) & a) >> 21) << 19;
		b |= (((1 << 22) & a) >> 22) << 18;
		b |= (((1 << 23) & a) >> 23) << 17;
		return b;
	}

	static int tt_bslash(int a) {
		int b = 0;
		b |= (((1 << 0) & a) >> 0) << 2;
		b |= (((1 << 1) & a) >> 1) << 1;
		b |= (((1 << 2) & a) >> 2) << 0;
		b |= (((1 << 3) & a) >> 3) << 7;
		b |= (((1 << 4) & a) >> 4) << 6;
		b |= (((1 << 5) & a) >> 5) << 5;
		b |= (((1 << 6) & a) >> 6) << 4;
		b |= (((1 << 7) & a) >> 7) << 3;
		b |= (((1 << 8) & a) >> 8) << 10;
		b |= (((1 << 9) & a) >> 9) << 9;
		b |= (((1 << 10) & a) >> 10) << 8;
		b |= (((1 << 11) & a) >> 11) << 15;
		b |= (((1 << 12) & a) >> 12) << 14;
		b |= (((1 << 13) & a) >> 13) << 13;
		b |= (((1 << 14) & a) >> 14) << 12;
		b |= (((1 << 15) & a) >> 15) << 11;
		b |= (((1 << 16) & a) >> 16) << 18;
		b |= (((1 << 17) & a) >> 17) << 17;
		b |= (((1 << 18) & a) >> 18) << 16;
		b |= (((1 << 19) & a) >> 19) << 23;
		b |= (((1 << 20) & a) >> 20) << 22;
		b |= (((1 << 21) & a) >> 21) << 21;
		b |= (((1 << 22) & a) >> 22) << 20;
		b |= (((1 << 23) & a) >> 23) << 19;
		return b;
	}

	static int tt_slash(int a) {
		int b = 0;
		b |= (((1 << 0) & a) >> 0) << 6;
		b |= (((1 << 1) & a) >> 1) << 5;
		b |= (((1 << 2) & a) >> 2) << 4;
		b |= (((1 << 3) & a) >> 3) << 3;
		b |= (((1 << 4) & a) >> 4) << 2;
		b |= (((1 << 5) & a) >> 5) << 1;
		b |= (((1 << 6) & a) >> 6) << 0;
		b |= (((1 << 7) & a) >> 7) << 7;
		b |= (((1 << 8) & a) >> 8) << 14;
		b |= (((1 << 9) & a) >> 9) << 13;
		b |= (((1 << 10) & a) >> 10) << 12;
		b |= (((1 << 11) & a) >> 11) << 11;
		b |= (((1 << 12) & a) >> 12) << 10;
		b |= (((1 << 13) & a) >> 13) << 9;
		b |= (((1 << 14) & a) >> 14) << 8;
		b |= (((1 << 15) & a) >> 15) << 15;
		b |= (((1 << 16) & a) >> 16) << 22;
		b |= (((1 << 17) & a) >> 17) << 21;
		b |= (((1 << 18) & a) >> 18) << 20;
		b |= (((1 << 19) & a) >> 19) << 19;
		b |= (((1 << 20) & a) >> 20) << 18;
		b |= (((1 << 21) & a) >> 21) << 17;
		b |= (((1 << 22) & a) >> 22) << 16;
		b |= (((1 << 23) & a) >> 23) << 23;
		return b;
	}


	static int swap(int a) {
		int b = 0;
		b |= (((1 << 0) & a) >> 0) << 16;
		b |= (((1 << 1) & a) >> 1) << 17;
		b |= (((1 << 2) & a) >> 2) << 18;
		b |= (((1 << 3) & a) >> 3) << 19;
		b |= (((1 << 4) & a) >> 4) << 20;
		b |= (((1 << 5) & a) >> 5) << 21;
		b |= (((1 << 6) & a) >> 6) << 22;
		b |= (((1 << 7) & a) >> 7) << 23;
		b |= (((1 << 8) & a) >> 8) << 8;
		b |= (((1 << 9) & a) >> 9) << 9;
		b |= (((1 << 10) & a) >> 10) << 10;
		b |= (((1 << 11) & a) >> 11) << 11;
		b |= (((1 << 12) & a) >> 12) << 12;
		b |= (((1 << 13) & a) >> 13) << 13;
		b |= (((1 << 14) & a) >> 14) << 14;
		b |= (((1 << 15) & a) >> 15) << 15;
		b |= (((1 << 16) & a) >> 16) << 0;
		b |= (((1 << 17) & a) >> 17) << 1;
		b |= (((1 << 18) & a) >> 18) << 2;
		b |= (((1 << 19) & a) >> 19) << 3;
		b |= (((1 << 20) & a) >> 20) << 4;
		b |= (((1 << 21) & a) >> 21) << 5;
		b |= (((1 << 22) & a) >> 22) << 6;
		b |= (((1 << 23) & a) >> 23) << 7;
		return b;
	}

	static int swap_rot90(int a) {
		return swap(rot90(a));
	}

	static int swap_rot180(int a) {
		return swap(rot180(a));
	}

	static int swap_rot270(int a) {
		return swap(rot270(a));
	}

	static int swap_tt_fuggoleges(int a) {
		return swap(tt_fuggoleges(a));
	}

	static int swap_tt_vizszintes(int a) {
		return swap(tt_vizszintes(a));
	}

	static int swap_tt_bslash(int a) {
		return swap(tt_bslash(a));
	}

	static int swap_tt_slash(int a) {
		return swap(tt_slash(a));
	}

	static int id(int a) {
		return a;
	}
}