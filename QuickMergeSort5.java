package quickMergeSort;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import tools.MyArrayUtil;
import tools.TestTools;

public class QuickMergeSort5<E extends Comparable> {
	protected E[] array;
	private final int SMALL = 20;

	ExecutorService executor;
	int threadsNum, arrayLength, sortSection, left, right;
	int restPivot; //マージするときに余った部分の仕切り
	LinkedList<Callable<Object>> workers;

	TestTools tt;

	public QuickMergeSort5(){

		threadsNum = Runtime.getRuntime().availableProcessors();
		tt = new TestTools();

	}

	public void sort(E[] array){

		this.array = array;
		arrayLength = array.length;

		sortSection = arrayLength / (threadsNum * 2);
		left = 0;  right = sortSection - 1;
		restPivot = -1;

		executor = Executors.newFixedThreadPool(threadsNum);
		workers = new LinkedList<Callable<Object>>();


		//各区分をクイックソート
		parallelQuickSort();

		//区分ごとにソートされたところをマージ処理していく
		try {
			parallelMergeSort();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}


	}

	private void parallelQuickSort(){
		//クイックソートをする
		while(right < arrayLength-1){
			workers.add(Executors.callable(new QuickSortWorker(left,right)));
			left = right + 1;
			right =  sortSection + right;
		}
		//最後の区分だけ特別に処理する
		workers.add(Executors.callable(new QuickSortWorker(left,arrayLength-1)));

		restPivot = left;

		try {
			executor.invokeAll(workers);  //workersの仕事を実行し、終わるまで待機
		} catch (InterruptedException e) {
			// TODO 自動生成された catch ブロック
			e.printStackTrace();
		}
	}

	private void parallelMergeSort() throws InterruptedException {
		sortSection = sortSection * 2 ;  //一度にマージする範囲

		while(sortSection <= arrayLength - 1){
			workers.clear();
			left = 0;  right = sortSection - 1;

			while(right < arrayLength-1){
				workers.add(Executors.callable(new MergeSortWorker(left,right)));
				left = right + 1;
				right = right + sortSection;

				if(right >= arrayLength-1){  //余り部分の処理 left < arrayLength
					if((arrayLength - left) <= sortSection/2){  //同じ範囲のソート防止
						restPivot = left;
						break;
					}

					workers.add(Executors.callable(new MergeSortWorker(left,restPivot-1,arrayLength - 1)));
					restPivot = left;	//余ったところの仕切りは独自のものが必要なので保存しておく
					break;
				}
			}

			executor.invokeAll(workers);   ////workersの仕事を実行し、終わるまで待機
			sortSection = sortSection * 2;	//マージする区分を拡大する

			threadsNum = threadsNum/2; //不必要なスレッドを減らしていく
			((ThreadPoolExecutor)executor).setCorePoolSize(threadsNum);
		}

		executor.shutdown();
		//最後のmerge
		merge(0,restPivot-1,arrayLength - 1,new Object[arrayLength]);
	}


	class MergeSortWorker implements Runnable{
		int left,right,mid;
		Object[] buff;
		public MergeSortWorker(int left,int right){
			this.left = left;
			this.right = right;
			mid = (left + right) / 2;
			buff = new Object[right - left+1];
		}

		public MergeSortWorker(int left,int mid,int right){
			this.left = left;
			this.right = right;
			this.mid = mid;
			buff = new Object[right - left+1];
		}
		public void run(){
			merge(left,mid,right,buff);
		}
	}

	class QuickSortWorker implements Runnable {
		int left,right;
		public QuickSortWorker(int left,int right){
			this.left = left;
			this.right = right;
		}

		public void run() {
			quickSort(left,right);
		}
	}


	@SuppressWarnings("unchecked")
	public void merge(int left,int mid,int right,Object[] buff){
		int i = left,j = mid + 1,k = 0;

		while(i <= mid && j <= right) {
			if(array[i].compareTo(array[j]) < 0)
				buff[k++] = array[i++];
			else
				buff[k++] = array[j++];
		}

		while(i <= mid)
			buff[k++] = array[i++];
		while(j <= right)
			buff[k++] = array[j++];
		for(i = left;i <= right; i++)
			array[i] = (E) buff[i - left];
	}


	@SuppressWarnings("unchecked")
	public int partition(int left,int right){
		int i = left - 1, j = right;

		E pivot  = array[right];
		while(true){
			do{
				i++;
			}while(array[i].compareTo(pivot) < 0);
			do{
				j--;
				if(j < left) break;
			}while(pivot.compareTo(array[j]) < 0);
			if(i >= j) break;
			swap(i,j);
		}

		swap(i,right);
		return i;
	}

	public void swap(int i,int j){
		E temp = array[i];
		array[i] = array[j];
		array[j] = temp;
	}

	@SuppressWarnings("unchecked")
	public void insertSort(int left,int right){
		int i,j;
		E temp;
		for(i = left + 1; i <=right;i++) {
			temp = array[i];
			j = i;
			while(j > left && temp.compareTo(array[j-1])< 0){
				array[j] = array[j-1];
				j--;
			}
			array[j] = temp;
		}
	}

	@SuppressWarnings("unchecked")
	public void quickSort(int left,int right){
		int i;
		if(right <= left) return;

		if(right <= left + SMALL)
			insertSort(left,right);
		else{

			swap((left + right) / 2,right - 1);
			if(array[right - 1].compareTo(array[left]) < -1)
				swap(right-1,left);
			if(array[right].compareTo(array[left]) < -1)
				swap(right,left);
			if(array[right].compareTo(array[right-1]) < -1)
				swap(right,right);

			i = partition(left,right);
			quickSort(left, i - 1);
			quickSort(i + 1 , right);
		}
	}

}
