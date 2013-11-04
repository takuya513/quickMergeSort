package quickMergeSort;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import quickSort.QuickSort;
import sort.Sort;
import tools.MyArrayUtil;
import tools.MyData;
import tools.MyInteger;

public class QuickMergeSort2<E extends Comparable> implements Sort<E> {
	protected E[] array;
	private final int SMALL = 10;

	ExecutorService executor;
	int threadsNum, arrayLength, sectionOfSort, pivotOfEnd, pos, pos2;
	int pivotOfRest = -1; //マージするときに余った部分の仕切り
	final ArrayList<Callable<Object>> workers;

	public QuickMergeSort2(){
		threadsNum = Runtime.getRuntime().availableProcessors()-1;
		//threadsNum =3;
		executor = Executors.newFixedThreadPool(threadsNum);
		workers = new ArrayList<Callable<Object>>(threadsNum);

	}

	public void sort(E[] array){
		this.array = array;
		arrayLength = array.length;
		sectionOfSort = array.length / threadsNum;
		pivotOfEnd = 0; pos = 0;  pos2 = sectionOfSort - 1;

		parallelQuickSort();

		//ここからマージ処理を行う

		try {
			parallelMergeSort();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}


	}

	private void parallelQuickSort(){
		//クイックソートをする
		while(pos2 < arrayLength){
			workers.add(Executors.callable(new QuickSortWorker(pos,pos2)));
			pos = pos2 + 1;
			pos2 =  sectionOfSort + pos2;
		}

		//最後の区分だけ特別に処理する
		workers.add(Executors.callable(new QuickSortWorker(pos,arrayLength-1)));
		pivotOfRest = pos;

		try {
			executor.invokeAll(workers);
		} catch (InterruptedException e) {
			// TODO 自動生成された catch ブロック
			e.printStackTrace();
		}

	}

	private void parallelMergeSort() throws InterruptedException {
		int expSec = 2;
		int lastLenOfRestSection = -1;  //繰り返し防止を確認するための変数
		pos = 0;pos2 = sectionOfSort * expSec - 1 ;
		
		while(pos2 <= arrayLength - 1){
			workers.clear();
			threadsNum = threadsNum/2;
			((ThreadPoolExecutor)executor).setCorePoolSize(threadsNum);

			while(true){
				workers.add(Executors.callable(new MergeSortWorker(pos,pos2)));

				if(pos2 == arrayLength-1)
					break;

				pos = pos2 + 1;
				pivotOfEnd = pos;
				pos2 = pos2 + sectionOfSort*expSec;

				//余り部分の処理
				if(pos2 > arrayLength-1){
//					if(isSameSort(lastLenOfRestSection)){
//						System.out.println("Treu");
//						break;
//					}
					if(pivotOfRest != -1)  //最初の余り部分でなければ
						workers.add(Executors.callable(new MergeSortWorker(pos,pivotOfRest-1,arrayLength - 1)));

					pivotOfRest = pos;
					lastLenOfRestSection = arrayLength - pos;
					break;
				}
			}

			expSec = expSec * 2;
			executor.invokeAll(workers);
			pos = 0;pos2 = sectionOfSort * expSec - 1 ;
		}

		executor.shutdown();
		//最後のmerge
		merge(0,pivotOfEnd-1,arrayLength - 1,new LinkedList<E>());
	}




	//前回同じ範囲でソートしたかcheak
	public boolean isSameSort(int lastLenOfRestSection){
		if((arrayLength - pos) == lastLenOfRestSection)
			return true;

		return false;
	}

	public  void merge(int left,int mid,int right,LinkedList<E> buff){
		int i = left,j = mid + 1;

		while(i <= mid && j <= right) {
			if(array[i].compareTo(array[j]) < 0){
				buff.add(array[i]); i++;
			}else{
				buff.add(array[j]); j++;
			}
		}

		while(i <= mid) { buff.add(array[i]); i++;}
		while(j <= right) { buff.add(array[j]); j++;}
		for(i = left;i <= right; i++){ array[i] = buff.remove(0);}
	}


	class MergeSortWorker implements Runnable{
		int left,right,mid;
		LinkedList<E> buff;
		public MergeSortWorker(int left,int right){
			this.left = left;
			this.right = right;
			mid = (left + right) / 2;
			buff = new LinkedList<E>();
		}

		public MergeSortWorker(int left,int mid,int right){
			this.left = left;
			this.right = right;
			this.mid = mid;
			buff = new LinkedList<E>();
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
