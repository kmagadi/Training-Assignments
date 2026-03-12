import java.util.*;

public class GameOfLife {

    public static void nextState(int[][] board) {

        int m = board.length;
        int n = board[0].length;

        int[][] next = new int[m][n];

        int[] dx = {-1,-1,-1,0,0,1,1,1};
        int[] dy = {-1,0,1,-1,1,-1,0,1};

        for (int i = 0; i < m; i++) {

            for (int j = 0; j < n; j++) {

                int live = 0;

                for (int d = 0; d < 8; d++) {

                    int x = i + dx[d];
                    int y = j + dy[d];

                    if (x >= 0 && x < m && y >= 0 && y < n && board[x][y] == 1)
                        live++;
                }

                if (board[i][j] == 1) {

                    if (live == 2 || live == 3)
                        next[i][j] = 1;
                }
                else {

                    if (live == 3)
                        next[i][j] = 1;
                }
            }
        }

        for (int i = 0; i < m; i++)
            System.arraycopy(next[i], 0, board[i], 0, n);
    }

    public static void main(String[] args) {

        int[][] board = {
                {0,1,0},
                {0,0,1},
                {1,1,1},
                {0,0,0}
        };

        nextState(board);

        for (int[] row : board)
            System.out.println(Arrays.toString(row));
    }
}