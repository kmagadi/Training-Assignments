import java.util.*;

public class MineSweeper {

    public static void generate(char[][] board) {

        int m = board.length;
        int n = board[0].length;

        int[] dx = {-1,-1,-1,0,0,1,1,1};
        int[] dy = {-1,0,1,-1,1,-1,0,1};

        for (int i = 0; i < m; i++) {

            for (int j = 0; j < n; j++) {

                if (board[i][j] == 'M')
                    continue;

                int count = 0;

                for (int d = 0; d < 8; d++) {

                    int x = i + dx[d];
                    int y = j + dy[d];

                    if (x >= 0 && x < m && y >= 0 && y < n && board[x][y] == 'M')
                        count++;
                }

                board[i][j] = (char)(count + '0');
            }
        }
    }

    public static void main(String[] args) {

        char[][] board = {
                {'E','E','M'},
                {'E','E','E'},
                {'M','E','E'}
        };

        generate(board);

        for (char[] row : board)
            System.out.println(Arrays.toString(row));
    }
}